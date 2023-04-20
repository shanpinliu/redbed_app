#==============================================================================
#GENERATE RESULTS TABLE
#==============================================================================
# -*- coding: utf-8 -*-

import time, re, psycopg2, csv, json


def ReadTxtName(rootdir):
    lines = []
    with open(rootdir, 'r', encoding='utf-8', errors='ignore') as file_to_read:
        while True:
            line = file_to_read.readline()
            if not line:
                break        
            line = line.strip('\n')
            lines.append(line)
    return lines

def interpose_results(dblist, credentials, config):
    
    start_time = time.time()
    
    """
    file = ReadTxtName(r'D:\python-script\macrostrat\stromatolites_demo-master\output\wrong_names.txt')
    wrong_names = []
    for i in file:
        if i != '':
            wrong_names.append(i)
    """
    
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    #NEW RESULTS TABLE
    cursor.execute("""
        DROP TABLE IF EXISTS {results} CASCADE;
        CREATE TABLE {results}(
            target_id int,
            docid text,
            sentid int,
            target_word text,
            strat_phrase_root text,
            strat_flag text,
            strat_name_id  text,
            int_name text,
            int_id int,
            age_sum text,
            source text,
            phrase text,
            is_strat_name text DEFAULT 'yes',
            in_ref text
            );
    """.format(**dblist))
    connection.commit()
    
    #TMP RESULTS TABLE
    cursor.execute("""
        DROP TABLE IF EXISTS {results_new};
    """.format(**dblist))
    
    #push drop/create to the database
    connection.commit()
    
    #gather results from the same-sentence inferences
    cursor.execute(""" 
        INSERT INTO {results} (target_id, docid, sentid, target_word, strat_phrase_root,strat_flag,strat_name_id, int_name, int_id, age_sum, phrase, in_ref) 
    		(SELECT target_id, docid, sentid,  target_word, strat_phrase_root,strat_flag,strat_name_id, int_name, int_id, age_sum, sentence, in_ref
    				FROM {strat_target}
    				WHERE ((num_phrase=1 AND @(target_distance)<51) 
    				OR   (target_relation='parent' AND num_phrase <8 AND @(target_distance)<51)
    				OR   (target_relation='child'  AND num_phrase <8 AND @(target_distance)<51)))""".format(**dblist))
     
    #push insertions
    connection.commit()
    
    #mark these inferences as coming from same sentence
    cursor.execute("""
        UPDATE {results} SET source='in_sent' WHERE source IS NULL 
       """.format(**dblist))
    
    #push update
    connection.commit()
    
    #gather results from the near-sentence inferences
    cursor.execute(""" 
        INSERT INTO {results} (target_id, docid, sentid, target_word, strat_phrase_root,strat_flag,strat_name_id, int_name, int_id, age_sum, phrase, in_ref) 
    		(SELECT target_id, docid, sentid,  target_word, strat_phrase_root, strat_flag, strat_name_id, int_name, int_id, age_sum, words_between, in_ref
    				FROM {strat_target_distant} 
    				WHERE num_phrase=1)""".format(**dblist))
      
    #push insertions
    connection.commit()
    
    #mark these inferences as coming from near sentence
    cursor.execute("""
        UPDATE {results} SET source='out_sent' WHERE source IS NULL 
       """.format(**dblist))
    
    #remove non-unique rows
    cursor.execute("""
        CREATE TABLE {results_new} AS (SELECT DISTINCT * FROM {results})
       """.format(**dblist))
    
    
    #adopt tmp results table
    cursor.execute("""
        DROP TABLE {results}
       """.format(**dblist))
    
    cursor.execute("""
        ALTER TABLE {results_new} RENAME TO {results};
       """.format(**dblist))
    
    
    #add serial primary key
    cursor.execute("""
        ALTER TABLE {results} ADD COLUMN result_id serial PRIMARY KEY;
       """.format(**dblist))
    
    #push updates
    connection.commit()
    
    #list of known and troublesome ligatures
    #weird_strings = [['\xef\xac\x82', 'fl'], ['\xef\xac\x81', 'fi']]
    
    
    #IMPORT THE RESULTS - SIMPLE CHECK FOR STRAT NAME MENTION VALIDITY 
    cursor_main = connection.cursor()
    cursor_main.execute(""" SELECT * FROM {results} WHERE strat_flag = 'mention'; """.format(**dblist))
    
    #test=[]
    
    for line in cursor_main:
        #collect individual elements from the results dump
        target_id, docid, sentid, target_word, strat_phrase_root, strat_flag, strat_name_id, int_name, int_id, age_sum, source, phrase, mention_check, in_ref, result_id = line
        
        checked=[]
        #ligature replacement
        """
        for ws in weird_strings:
            if ws[0] in phrase:
                phrase=phrase.replace(ws[0],ws[1])
        """
        #find all mentions of strat_phrase_root
        matches=[m.start() for m in re.finditer(strat_phrase_root,phrase)]
        
        #loop through matches
        for m in matches:
            #lets look at the word that follows the potential strat name
            tocheck = phrase[m+len(strat_phrase_root)+1:]
            tocheck=tocheck.split(' ')
            
            #capitalized word following strat name mention invalidates it. Exceptions include:
                #1) end of sentence  2) Series  3) parantheses
            if tocheck[0].lower()!=tocheck[0] and tocheck[0]!='Series' and tocheck[0][0]!='.' and tocheck[0]!='-LRB-' and tocheck[0]!='-RRB-':        
                checked.append('no')
            else:
                checked.append('yes')
        
        #update post gres table
        if 'yes' not in checked:
            cursor.execute("""
                UPDATE {results} SET is_strat_name = %s WHERE result_id = %s;""".format(**dblist),
                ('no',result_id)
               )
            
    #push update
    connection.commit()
    
    #write culled results to CSV
    cursor.execute("""
             SELECT result_id, docid, sentid, target_word, strat_phrase_root, strat_flag, strat_name_id, int_name, int_id, age_sum, in_ref, source, phrase
            	FROM {results} 
            	WHERE (is_strat_name='yes' AND source='in_sent')
               OR (is_strat_name='yes' AND source='out_sent' AND in_ref='no')
         """.format(**dblist))
         
    results=cursor.fetchall()
    
    with open('./output/{results}.csv'.format(**dblist), 'w', encoding="utf-8") as outcsv:   
        #configure writer to write standard csv file
        writer = csv.writer(outcsv, delimiter=',', quoting=csv.QUOTE_ALL, lineterminator='\n')
        writer.writerow(['result_id','docid','sentid','target_word','strat_phrase_root','strat_flag','strat_name_id','int_name','int_id','in_ref','source','phrase'])
        for item in results:
            phrase = item[12].replace("\\\\' \\\\'", ",")
            phrase = phrase.replace("\\\\'", "'")
            #Write item to outcsv
            writer.writerow([item[0], item[1], item[2],item[3], item[4], item[5],item[6], item[7], item[8], item[9], item[11], phrase])
    
    sentences = []
    for item in results:
        phrase = item[12].replace("\\\\' \\\\'", ",")
        phrase = phrase.replace("\\\\'", "'")
        sentences.append ({'result_id':item[0], 'docid':item[1],  'sentid':item[2], 'target_word':item[3], 'strat_phrase_root':item[4], 'strat_flag':item[5], 'strat_name_id':item[6], 'int_name':item[7], 'int_age':item[8], 'age_sum':item[9], 'in_ref':item[10], 'source':item[11], 'phrase':phrase})

    with open('./output/{results}.json'.format(**dblist), 'w', encoding='utf-8') as f:
        json.dump(sentences, f, indent=2, sort_keys=True, ensure_ascii=False) 
        
    print("results:",len(results))
    print("len sentences:",len(sentences))
    
    
    #close the postgres connection
    connection.close()
    
    elapsed_time = time.time() - start_time
    print ('\n ########### elapsed time: %d seconds ###########\n\n' %(elapsed_time))

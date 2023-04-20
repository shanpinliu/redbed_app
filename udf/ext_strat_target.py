   #==============================================================================
#DEFINE RELATIONSHIP BETWEEN TARGET ENTITIES AND STRATIGRAPHIC PHRASES
#==============================================================================

# ACQUIRE RELEVANT MODULES and DATA
#==============================================================================

# -*- coding: utf-8 -*-

import time, random, psycopg2, yaml, sys
from psycopg2.extensions import AsIs

def intial_strat_target(dblist, credentials):
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    #initalize the strat_target relationship table
    cursor.execute("""
        DELETE FROM {strat_target};
    """.format(**dblist))
    connection.commit()
    
    #creat index, this become surprisingly more efficient 
    cursor.execute(""" DROP INDEX IF EXISTS index_target_instances;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_strat_phrases;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_NLPname;""")
    connection.commit()
    
    cursor.execute(""" CREATE INDEX index_target_instances ON {target_instances} (docid, sentid);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_strat_phrases ON {strat_phrases} (docid, sentid, strat_name_id, age_agree);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_NLPname ON {NLPname} (docid, sentid);""".format(**dblist))
    connection.commit()
    
    #strat_phrases data dump
    cursor.execute("""
        SELECT  DISTINCT ON ({strat_phrases}.docid,
                {strat_phrases}.sentid,
                strat_phrase,
                phrase_start,
                phrase_end)
                
                {strat_phrases}.docid,
                {strat_phrases}.sentid,
                strat_phrase_root,
                strat_flag,
                strat_name_id,
                phrase_start,
                phrase_end,
                int_name,
                int_id,
                num_phrase,
                {NLPname}.words,
                {strat_phrases}.age_agree
                
        FROM    {strat_phrases}, {target_instances}, {NLPname}
        WHERE   {strat_phrases}.docid={target_instances}.docid
        AND     {strat_phrases}.sentid={target_instances}.sentid
        AND     {strat_phrases}.docid={NLPname}.docid
        AND     {strat_phrases}.sentid={NLPname}.sentid;
    """.format(**dblist))
    #convert list of tuples to list of lists
    strat_list=cursor.fetchall()
    strat_list = [list(elem) for elem in strat_list]
    print("strat_list:", len(strat_list))
    connection.commit()

    return strat_list
    

def load_strat_target(strat_list, shared):
    #tic
    start_time = time.time()
    
    dblist  = shared[0]
    credentials = shared[1]
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    connection.commit()
    
    #==============================================================================
    # DEFINING RELATIONSHIP BETWEEN STRATIGRAPHY ENTITY/MENTION AND TARGET
    #==============================================================================
    
    #strat_target_list=[]
    
    #loop through all sentences with strat entities/mentions
    all_ = len(strat_list)
    for idx, line in enumerate(strat_list):
        doc_id, sent_id, strat_phrase_root, strat_flag, strat_name_id, phrase_start, phrase_end, int_name, int_id, num_phrase, words, age_agree = line
        sentence = ' '.join(words)
        sentence = sentence.replace(r"\\' \\'", ",")
        sentence = sentence.replace(r"\\'", "'")
        #target_instances data dump
        cursor.execute("""
            SELECT  {target_instances}.docid,
                    {target_instances}.sentid,
                    target_word,
                    target_word_idx,
                    target_pose,
                    target_path,
                    target_parent,
                    target_children,
                    {NLPname}.words,
                    target_id
            FROM    {target_instances}, {NLPname}
            WHERE   {target_instances}.docid=%(my_docid)s 
            AND     {target_instances}.sentid=%(my_sentid)s
            AND     {target_instances}.docid={NLPname}.docid
            AND     {target_instances}.sentid={NLPname}.sentid;""".format(**dblist), {"my_docid": doc_id, "my_sentid": sent_id})
        
        #convert list of tuples to list of lists
        target_instances=cursor.fetchall()
        target = [list(elem) for elem in target_instances]

        
        #loop through all target instances in that sentence
        for idx2,elem in enumerate(target):
            doc_id, sent_id, target_word,target_word_idx,target_pose,target_path,target_parent,target_children, words, target_id = elem
            sent = ' '.join(words)
            sent = sent.replace(r"\\' \\'", ",")
            sent = sent.replace(r"\\'", "'")
            words = sent.split(' ')
            
            #is the stratigraphic entity/mention a PARENT or CHILD of the target instance?
            if list(set(target_parent) & set(range(phrase_start,phrase_end)))!=[]:
                target_relation='parent'
            elif list(set(sum(eval(target_children), [])) & set(range(phrase_start,phrase_end)))!=[]:
                target_relation='child'
            else:
                target_relation='na'
    
            #what is the word DISTANCE between the strat mention/entity and the target instance?
            target_distance=[max(target_word_idx)-i for i in range(phrase_start,phrase_end)]
            target_distance=target_distance+[min(target_word_idx)-i for i in range(phrase_start,phrase_end)] 
            
            # target found WITHIN the strat phrase (e.g. Upper Stromatolitic Carbonate Member)
            if sum(n > 0 for n in target_distance)!=0 and sum(n < 0 for n in target_distance)!=0:
                target_distance=0
            #target found BEHIND the strat phrase
            elif sum(n > 0 for n in target_distance)==0:
                target_distance = max(target_distance)
            #target found AHEAD of the strat_phrase
            else:
                target_distance = min(target_distance)
           
            #grab the bag of words
            if target_distance>1:
                words_between = words[phrase_end:phrase_end+(target_distance)]
            elif target_distance<-1:
                words_between = words[phrase_start+(target_distance):phrase_start]
            else:
                words_between='{}'
            
            #dump to local variable
            #strat_target_list.append([doc_id, sent_id, strat_phrase_root,num_phrase, target_relation,target_distance,sentence, strat_flag,phrase_start,phrase_end,int_name,int_id, words_between,target_word,target_word_idx])
            #write to PSQL table
            cursor.execute(""" 
                INSERT INTO {strat_target}(   docid,
                                              sentid,
                                              target_word,
                                              target_word_idx,
                                              strat_phrase_root,
                                              strat_flag,
                                              strat_name_id,
                                              strat_start,
                                              strat_end,
                                              int_name,
                                              int_id,
                                              num_phrase,
                                              target_relation,
                                              target_distance,
                                              words_between,
                                              sentence,
                                              age_agree,
                                              target_id)
                            VALUES (%s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s);""".format(**dblist),
                                    (doc_id, sent_id, target_word,
                                     target_word_idx, strat_phrase_root, strat_flag,
                                     strat_name_id,phrase_start,phrase_end,
                                     int_name,int_id,num_phrase,target_relation,
                                     target_distance,words_between,sentence,age_agree, target_id))
            
        print("main loop:", (idx+1)/all_, end='\r')
        
    connection.commit()
    
    #summary statistic    
    #success = 'number of strat-target tuples : %s' %len(strat_target_list)
    
    #summary of performance time
    elapsed_time = time.time() - start_time
    #print ('\n ###########\n\n %s \n elapsed time: %d seconds\n\n ###########\n\n' %(success,elapsed_time))
    print ('\n ########### elapsed time: %d seconds ###########\n\n' %(elapsed_time))
    
    
    #show a random result
    #r=random.randint(0,len(strat_target_list)-1); show = "\n".join(str(x) for x in strat_target_list[r][0:7]); 
    #print ("=========================\n" + show +  "\n=========================")
            

    
    
def alter_strat_target(dblist, credentials):
    print("run alter_strat_target")
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    cursor.execute(""" DROP INDEX IF EXISTS index_strat_target;""")
    connection.commit()
    cursor.execute(""" CREATE INDEX index_strat_target ON {strat_target} (strat_name_id);""".format(**dblist))
    connection.commit()
    
    #some sort of magic
    connection.set_isolation_level(0)
    cursor.execute("""  VACUUM ANALYZE {strat_target};
    """.format(**dblist))
    connection.commit()
    
    #==============================================================================
    # PROVIDE SUMMARIES FOR AGE-AGREEMENT BETWEEN STRAT_PHRASE AND MACROSTRAT STRAT_NAME
    #==============================================================================
    
    #initialize the age_agree column in strat_phrases
    cursor.execute(""" 
            UPDATE  {strat_target} 
            SET     age_sum = '-';
    """.format(**dblist))
    connection.commit()
    
    #gather distinct Macrostrat links
    cursor.execute("""
        SELECT DISTINCT (strat_name_id) FROM {strat_target};
    """.format(**dblist))
    
    #convert list of tuples to list of lists
    tocheck=cursor.fetchall()
    tocheck = [list(elem) for elem in tocheck]
    
    #find all instances of strat_name_id occuring in the age_check table
    cursor.execute("""
        WITH  query AS(SELECT DISTINCT (strat_name_id) FROM {strat_target})
                   SELECT {strat_phrases}.strat_name_id, {strat_phrases}.age_agree FROM {strat_phrases},query
                   		WHERE {strat_phrases}.strat_name_id=query.strat_name_id
                   		AND   {strat_phrases}.age_agree<>'-';
        """.format(**dblist))
    
    #convert list of tuples to list of lists    
    results=cursor.fetchall()
    results = [list(elem) for elem in results]
    
    #loop through all strat_name_ids and summarize age agreement discoveries
    for idx,name in enumerate(tocheck):
        tmp = [i for i in results if i[0]==name[0]]        
        ids = name[0].split('~')
    
        #initialize the age agreement list    
        counts = [[0] * 2 for i in range(len(ids))]
    
        #loop through all comparisons between a strat_name_id string and interval information
        for idx2,item in enumerate(tmp):        
            #consider each strat_name in the strat_name_string
            ans = item[1].split('~')
    
            #record whether its an allowable or disallowable match        
            for idx3,data in enumerate(ans):
                if data=='yes':
                    counts[idx3][0]+=1
                elif data=='no':
                    counts[idx3][1]+=1
        
        #record the age agreement summary                             
        tocheck[idx].extend([counts])
        
        #variables to push to PSQL database
        strat_name_id=name[0]
        str_counts=str(counts)
        
        #write to PSQL table
        cursor.execute(""" 
                UPDATE  {strat_target}
                SET     age_sum = %s
                WHERE   strat_name_id = %s;""".format(**dblist),
                (str_counts, strat_name_id)
                )
    connection.commit()
    
    # drop the indexes
    cursor.execute(""" DROP INDEX index_target_instances;""")
    connection.commit()
    cursor.execute(""" DROP INDEX index_strat_phrases;""")
    connection.commit()
    cursor.execute(""" DROP INDEX index_NLPname;""")
    connection.commit()
    cursor.execute(""" DROP INDEX index_strat_target;""")
    connection.commit()
    #close the postgres connection
    connection.close()

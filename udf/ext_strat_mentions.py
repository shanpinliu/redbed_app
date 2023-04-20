##==============================================================================
## LOOK FOR STRATIGRAPHIC NOMENCLATURE  - MENTION RECOGINITION
##==============================================================================

# ACQUIRE RELEVANT MODULES
#==============================================================================
# -*- coding: utf-8 -*-

import time, csv, random, psycopg2, re, yaml, string
from urllib.request import urlopen
from stop_words import get_stop_words
from psycopg2.extensions import AsIs
import codecs
import pandas as pd
import numpy as np


#function for dowloading CSVs from a URL
def download_csv( url ):
    
    #return variable
    dump_dict = {}
    
    #get strat_names from Macrostrat API
    dump = urlopen( url )
    dump = csv.reader(codecs.iterdecode(dump, 'utf-8'))
    
    #unpack downloaded CSV as list of tuples
    #--> length of VARIABLE == number of fields
    #--> length of VARIABLE[i] == number of rows
    #--> VARIABLE[i][0] = header name
    cols = list(zip(*dump))
    
    #key names correspond to field names (headers in the CSV file)
    for field in cols:
        dump_dict[field[0]]=field[1:]
        
    dump_dict['headers'] = sorted(dump_dict.keys())
    
    return dump_dict


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


def ReadCSVtoList(rootdir):
    file = pd.read_csv(rootdir,header=0)
    dump_dict = {}
    for i in file:
        dump_dict[i]=list(file[i])
    dump_dict['headers'] = sorted(dump_dict.keys())
    return dump_dict


def intial_strat_mention(dblist, credentials):
    #==============================================================================
    # CONNECT TO POSTGRES
    #==============================================================================
    
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    #initialize mentions
    cursor.execute("""DELETE FROM {strat_phrases} WHERE strat_flag='mention';""".format(**dblist))
    
    #import docid - strat_name tuples
    cursor.execute("""
        SELECT  * FROM {strat_dict};
    """.format(**dblist))
    connection.commit()
    
    strat_dict = cursor.fetchall()
    
    #convert list of tuples to list of lists
    strat_dict = [list(elem) for elem in strat_dict]
    
    #make a dictionary of docid-strat_name tuples
    doc_list=[]
    for i in strat_dict:
        doc_list.append([i[0], list(set(i[1]))])
        
    return doc_list

def load_strat_mention(doc_list, shared):
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
    # DEFINE STRATIGRPAHIC VARIABLES
    #==============================================================================
    
    #get interval_names from Macrostrat API
    #int_dict   = download_csv( 'https://macrostrat.org/api/defs/intervals?all&format=csv' )
    int_dict = ReadCSVtoList("./input/intervals.csv")
    
    #stop words
    stop_words = get_stop_words('english')
    #stop_words = [i.encode('ascii','ignore') for i in stop_words]  comment out
    alpha = list(string.ascii_lowercase);
    period1 = [i+'.' for i in alpha]
    period2 = [i+'|' for i in alpha]
    period3 = [i+str(j) for i in alpha for j in range(10)]
    period4 = ['~', '<', '>', '-LRB-', '-LLB-', ',', "\\'\\'", "\\\\'", "\\\\'\\\\'",'|', '*', ':', '?', '=', '#']
    period5 = ['lower', 'upper', 'middle', 'the', 'and', 'to', 'research', 'just', 'still', 'basin', 'basins']
    stop_words = stop_words + alpha + period1 + period2 + period3 + period4 + period5
    
    """
    file1 = ReadTxtName(r'D:\python-script\macrostrat\stromatolites_demo-multiprocess\output\wrong_words.txt')
    wrong_words = []
    for i in file1:
        if i != '':
            wrong_words.append(i)
            
    file2 = ReadTxtName(r'D:\python-script\macrostrat\stromatolites_demo-multiprocess\output\wrong_names.txt')
    wrong_names = []
    for i in file2:
        if i != '':
            wrong_names.append(i)

    file3 = ReadTxtName(r'D:\python-script\macrostrat\stromatolites_demo-multiprocess\output\weird_strings.txt')
    weird_strings2 = []
    for i in file3:
        if i != '':
            seg = i.split('==')
            if seg[0]!='' and seg[1]!='':
                weird_strings2.append(seg)
    """
    #user-defined variables
    #with open('./var/strat_variables.txt') as fid:
        #strat_variables = fid.readlines()
    #for i in strat_variables:
        #exec (i)
    
    #delimiter to separate strat_entities from strat_name_ids in strat_dict
    DICT_DELIM='$$$'
    
    #words indicating stratigraphic names
    strat_flags = ["Group", "Formation", "Member", "Supergroup", "Subgroup","Gp.", "Gr.", "Fm.", "Mbr.", "Mb.", "SGp.", "Gp", "Gr", "Fm", "Mbr", "Mb", "SGp", "formation", "member"]
        
    lith_flags = ["Dolomite", "Dolostone", "Limestone","Sandstone", "Shale", "Conglomerate", "Chert", "Mudstone",  "Claystone", "Siltstone", "Carbonate", "Chalk", 'Quartzite', "Marl"]
    
    strat_flags = strat_flags+lith_flags
                  
    #words indicating an age
    age_flags = ["Ga.", "Ga", "Gyr.", "Gyr", "Ma.", "Ma", "Myr.", "Myr", "m.y.", "m.y.r"]
    
    #weird_strings = weird_strings + weird_strings2
    
    #PRE-PROCESS: hack to replace weird strings
    #with a dictionary of stratigraphic entites mapped to a given document, find the mentions
    # i.e. find 'the Bitter Springs stromatolite' after identifying 'the Bitter Springs Formation'
    strat_flag = 'mention'
    
    discordance = ['overlain', 'overlying', 'overly', 'overburden', 'underlying', 'underlie', 'underlayer', 'substratum', 'unconformity', 'conformably', 'unconformably', 'overlapped', 'overlies', 'covered', 'covers', 'indistinguishable', 'distinguishable', 'succeeded', 'followed', 'upon'] 
    
    age_agree='-'
    #strat_list=[]
    #loop through documents with discoverd stratigraphic entities
    all_ = len(doc_list)
    for idx1,item in enumerate(doc_list):
        doc = item[0]
        #list of stratigraphic names associated with that document
        target_strat = item[1]
        
        #import sentences to mine - just restricted to sentences with target instance
        cursor.execute("""
            SELECT  DISTINCT ON ({target_instances}.docid,
                    {target_instances}.sentid)
                    
                    {target_instances}.docid,
                    {target_instances}.sentid,
                    {NLPname}.words
           FROM     {NLPname}, {target_instances}
           WHERE    {target_instances}.docid = %(my_docid)s
           AND      {NLPname}.docid = {target_instances}.docid
           AND      {NLPname}.sentid = {target_instances}.sentid;""".format(**dblist), {"my_docid": doc})
        target_sents=cursor.fetchall()
        #convert list of tuples to list of lists
        target_sents = [list(elem) for elem in target_sents]
        
        """
        for idx,line in enumerate(target_sents):
            line = list(line)
            sent = ' '.join(line[2])
            
            for ws in weird_strings:
                if ws[0] in sent:
                    #sentences[idx][2]=[word.replace(ws[0],ws[1]) for word in sentences[idx][2]] 
                    sent=sent.replace(ws[0],ws[1])
                    
            changed_words_ = sent.split(' ')
        
            changed_words = []
            for i in changed_words_:
                if i != ' ' and len(i)!=0:
                    changed_words.append(i)
                    
            line[2]=changed_words
            target_sents[idx] = tuple(line)
        """
        
        all_2 = len(target_sents)
        
        #loop through sentence data per document
        for idx2,line in enumerate(target_sents):
            
            doc_id, sent_id, words = line
            
            sent = ' '.join(words)
            sent = sent.replace(r"\\' \\'", ",")
            sent = sent.replace(r"\\'", "'")
            words = sent.split(' ')
            
            #check if there are words of contact relation, which should not be the target strata
            check_dis = [w for w in words if w in discordance]
            p = [pi for pi, p in enumerate(words) if p == '.']
            
            #print("                                                               doc_id, sent_id:", doc_id, sent_id, end='\r')
            sentence = ' '.join(words)
            for name in target_strat:
                #parse the (strat_name, strat_name_id) tuple
                strat_phrase=name.split(DICT_DELIM)[0]
                strat_phrase=strat_phrase.split(' ')
                strat_phrase=' '.join(strat_phrase[0:-1])
                strat_name_id=name.split(DICT_DELIM)[1]
                
                matches=[m.start() for m in re.finditer(r'\b' + strat_phrase + r'\b',sentence)]

                if matches:
                    #if at least one match is found, count number of spaces backward to arrive at word index
                    name_idx = [sentence[0:m].count(' ') for m in matches]
                    #remove double hits (i.e. stromatolitic-thrombolitic)
                    name_idx = list(set(name_idx))
                    name_idx.sort
                    #print(name_idx)
                    #split the strat mention into parts
                    name_part = strat_phrase.split(' ')
                    
                    #loop through all discoveries
                    for id3, i in enumerate(name_idx):
                        
                        if len(check_dis)>0:
                            p1 = [pi for pi in p if pi<i]
                            if len(p1) == 0:
                                p1 = 0
                            else:
                                p1 = p1[-1]
                            p2 = [pi for pi in p if pi>i]
                            if len(p2) == 0:
                                p2 = len(words)
                            else:
                                p2 = p2[0]
                            
                            check_dis = [w for w in words[p1:p2] if w in discordance]
                        
                        #record it as a mention if:
                        #   1) it is not at the end of the sentence
                        #   2) the phrase is not followed by a strat_flag
                        #       (this is to avoid duplication)
                        #   3) the mention is not part of garbled table e.g. 'Tumbiana Tumbiana Tumbiana Tumbiana'
                        if i>1 and i<len(words)-len(name_part) and words[i+len(name_part)] not in strat_flags and words[i] != words[i+1] and len(check_dis) == 0:
                            int_name='na'
                            int_id='0'
                            
                            #look to see if there is an interval name before the mention
                            #for j 
                            if words[i-1] in int_dict['name']:
                                #record this interval name
                                int_name=[words[i-1]]
                                if words[i-2] in int_dict['name'] or words[i-2].lower() in ['late', 'early', 'middle']:
                                    int_name.insert(0, words[i-2]) 
    
                                list_int_name = int_name
                                int_name = ' '.join(int_name)
    
                                #list comprehensions to record interval id
                                if int_name in int_dict['name']:
                                    locations1 = [k for k, t in enumerate(int_dict['name']) if t==list_int_name[-1]]
                                    int_id = [int_dict['int_id'][I] for I in locations1]
                                    int_id=int_id[0]
                                elif list_int_name[-1] in int_dict['name']:
                                    locations1 = [k for k, t in enumerate(int_dict['name']) if t==list_int_name[-1]]
                                    int_id = [int_dict['int_id'][I] for I in locations1]
                                    int_id=int_id[0]
                                break
                            
                            #look to see if there is an age_flag before the mention
                            elif words[i-1] in age_flags:
                                #record age flag with its preceding word (most likely a number)
                                int_name = [words[i-2], words[i-1]]
                                
                                if (i-2)>0:
                                    if re.findall(r'\d+', words[i-3])!=[] or words[i-3] in ['-', '—', '^', 'to', 'and', '±', '+']:
                                        int_name.insert(0, words[i-3])
                                if (i-3)>0:
                                    if re.findall(r'\d+', words[i-4])!=[] or words[i-4] in ['-', '—', '^', 'to', 'and', '±', '+']:
                                        int_name.insert(0, words[i-4])
                                    
                                int_name = ' '.join(int_name)
                                
                                int_name = int_name.replace('-', ' ')
                                int_name = int_name.replace('—', ' ')
                                int_name = int_name.replace('~', ' ')
                                int_name = int_name.replace('+', '±')
                                int_name = int_name.replace('<', ' ')
                                int_name = int_name.replace('e', ' ')
                                int_name = int_name.replace('>', ' ')
                                int_name = int_name.replace('^', ' ')
                                
                                int_root = int_name.split(' ')[-1]
                                
                                if '±' in int_name:
                                    int_name_ = int_name.split('±')[0]
                                    int_name_ = int_name_.split(' ')
                                else:
                                    int_name_ = int_name.split(' ')[0:-1]
                                
                                _int_name = []
                                for a in int_name_:
                                    try:
                                        age=float(a)
                                    except:
                                        age=0
                                    if 0 < age < 4000:
                                        _int_name.append(str(age))
                                int_name = _int_name + [int_root]
                                int_name = ' '.join(int_name) 
                                break
                            
                            #record where mention is found
                            max_word_id = str(i+len(name_part))
                            min_word_id = str(i)
                            #add to local variable
                            #strat_list.append('\t'.join(str(x) for x in [idx2, doc_id, sent_id,name.split(DICT_DELIM)[0], strat_phrase,strat_flag, min_word_id, max_word_id, strat_name_id,int_name,int_id, sentence, age_agree]))
                            
                            #write to PSQL table
                            cursor.execute(""" 
                                INSERT INTO {strat_phrases}(    docid,
                                                              sentid,
                                                              strat_phrase,
                                                              strat_phrase_root,
                                                              strat_flag,
                                                              phrase_start,
                                                              phrase_end,
                                                              strat_name_id,
                                                              int_name,
                                                              int_id,
                                                              sentence,
                                                              age_agree)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(**dblist),
                                (doc_id, sent_id, name.split(DICT_DELIM)[0], strat_phrase, strat_flag, min_word_id, max_word_id, strat_name_id, int_name, int_id, sentence, age_agree)
                                )
            print("main loop:", (idx2+1)/all_2, end='\r')
        print("main loop:", (idx1+1)/all_, end='\r')
    
    #push insertions to the database
    connection.commit()
    
    #summary statistic    
    #success = 'number of stratigraphic mentions : %s' %len(strat_list)
    
    #summary of performance time
    elapsed_time = time.time() - start_time
    #print ('\n ###########\n\n %s \n elapsed time: %d seconds\n\n ###########\n\n' %(success,elapsed_time))
    print ('\n\n ########### elapsed time: %d seconds ###########\n\n' %(elapsed_time))
    
    #print out random result
    #r=random.randint(0,len(strat_list)-1); show = "\n".join(str(x) for x in strat_list[r].split('\t')); print ("=========================\n" + show + "\n=========================")


    
    
def alter_strat_mention(dblist, credentials):
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    #some sort of magic
    connection.set_isolation_level(0)
    cursor.execute("""  VACUUM ANALYZE {strat_phrases};
    """.format(**dblist))
    connection.commit()
    
    connection.set_isolation_level(0)
    cursor.execute("""  VACUUM ANALYZE {target_instances};
    """.format(**dblist))
    connection.commit()
         
    
    #summarize the number of DISTINCT strat_name_roots found in a given sentence
    cursor.execute("""  WITH  query AS(SELECT docid, sentid,
                                      COUNT(DISTINCT strat_phrase_root) AS count
                                      FROM {strat_phrases}
                                      GROUP BY docid,sentid)
                                
                        UPDATE {strat_phrases}
                            SET num_phrase = query.count
                            FROM query
                            WHERE {strat_phrases}.docid = query.docid
                            AND {strat_phrases}.sentid = query.sentid""".format(**dblist))
    connection.commit()
    
    #summarize the number of DISTINCT strat_name_roots found for a given document
    cursor.execute("""  WITH  query AS(SELECT docid,
                                      COUNT(DISTINCT strat_phrase_root) AS count
                                      FROM {strat_phrases}
                                      GROUP BY docid)
                                
                        UPDATE {target_instances}
                            SET num_strat_doc = query.count
                            FROM query
                            WHERE {target_instances}.docid = query.docid""".format(**dblist))
    connection.commit()      
    
    #close the postgres connection
    connection.close()

#==============================================================================
#DEFINE RELATIONSHIP BETWEEN TARGET ENTITIES AND DISTANT STRATIGRAPHIC PHRASES
#==============================================================================

# ACQUIRE RELEVANT MODULES and DATA
#==============================================================================

# -*- coding: utf-8 -*-

import time, psycopg2
from operator import itemgetter


def intial_strat_target_distant(dblist, credentials):
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    #initalize the strat_target_distant relationship table
    cursor.execute("""
        DELETE FROM {strat_target_distant};""".format(**dblist))
    connection.commit()
    
    #some sort of magic
    connection.set_isolation_level(0)
    cursor.execute("""  VACUUM ANALYZE {target_instances};
    """.format(**dblist))
    connection.commit()
    
    #some sort of magic
    connection.set_isolation_level(0)
    cursor.execute("""  VACUUM ANALYZE {strat_phrases};
    """.format(**dblist))
    connection.commit()
    
    #some sort of magic
    connection.set_isolation_level(0)
    cursor.execute("""  VACUUM ANALYZE {NLPname};
    """.format(**dblist))
    connection.commit()
    
def prepare_strat_target_distant(dblist, credentials):
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    cursor.execute(""" DROP INDEX IF EXISTS index_target_instances;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_strat_phrases;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_NLPname;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_strat_target;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_strat_target_distant;""")
    connection.commit()
    
    #creat index, this become surprisingly more efficient 
    cursor.execute(""" CREATE INDEX index_target_instances ON {target_instances} (docid, sentid, target_id, num_strat_doc);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_strat_phrases ON {strat_phrases} (docid, sentid, strat_phrase_root, strat_name_id);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_NLPname ON {NLPname} (docid, sentid);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_strat_target ON {strat_target} (docid, sentid, target_id);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_strat_target_distant ON {strat_target_distant} (docid, sentid, strat_name_id);""".format(**dblist))
    connection.commit()
    
    
    print("begin prepare_strat_target_distant")
    #list of docids with orphaned targets
    cursor.execute("""
        SELECT  DISTINCT ON ({target_instances}.docid)
                {target_instances}.docid
        FROM    {target_instances}
        WHERE   {target_instances}.target_id
                NOT IN (select {strat_target}.target_id from {strat_target})
        AND     {target_instances}.num_strat_doc<>0
        AND {target_instances}.docid NOT IN (select distinct on ({strat_target_distant}.docid) {strat_target_distant}.docid from {strat_target_distant})
        ORDER BY {target_instances}.docid ASC, {target_instances}.sentid ASC
    """.format(**dblist))
    
    docs=cursor.fetchall()
    #convert list of tuples to list of lists
    sentences = [list(elem) for elem in docs]
    print("num docs:",len(docs))
    connection.commit()
    
    return sentences



def load_strat_target_distant(data):
    #tic
    start_time = time.time()
    
    sentences = data[0:-2]
    dblist  = data[-2]
    credentials = data[-1]
    
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    
    #==============================================================================
    # FIND STRATIGRAPHIC PHRASES NEAREST TO ORPHAN TARGET INSTANCES
    #==============================================================================
    
    #how many sentences back from orphan to look for stratigraphic phrases
    strat_distance = 3  
    
    #initialize the dump variable
    #strat_target_distant=[]
    
    target_cursor=connection.cursor()
    strat_cursor = connection.cursor()
    sent_cursor = connection.cursor()
    
    #loop through document list
    all_ = len(sentences)
    for idx,doc in enumerate(sentences):
        try:
            #orphaned targets from a given document
            target_cursor.execute("""
            SELECT  DISTINCT ON ({target_instances}.docid,
                    {target_instances}.sentid,
                    {target_instances}.target_word_idx)
            
                    {target_instances}.docid,
                    {target_instances}.sentid,
                    target_word,
                    target_word_idx,
                    target_parent,
                    target_children,
                    sentence,
                    target_id
            FROM    {target_instances}
            WHERE   {target_instances}.target_id 
                    NOT IN (select {strat_target}.target_id from {strat_target}) 
            AND     {target_instances}.docid=%(my_docid)s 
        """.format(**dblist), {
            "my_docid": doc[0]
            })
            #convert list of tuples to list of lists
            tmp_target = target_cursor.fetchall()
            tmp_target = [list(elem) for elem in tmp_target]
            tmp_target = sorted(tmp_target, key=itemgetter(0, 1))
            
            #define the sentences where those instances come from
            sentids = [item[1] for item in tmp_target]
        
            #gather all stratigraphic phrases from docid that occur before the deepest orphan
            sent_query = max(sentids)
            
            #strat_phrases from document that precede the orphan deepest into the document
            strat_cursor.execute(""" 
                SELECT DISTINCT ON (docid, sentid, strat_phrase_root, strat_name_id)
                        docid, sentid, strat_phrase_root, strat_flag, num_phrase, strat_name_id,int_name,int_id,age_agree from {strat_phrases}
                        WHERE docid=%s
                        AND sentid<%s;""".format(**dblist),
                     (doc[0], sent_query)
                     )
        
            #convert list of tuples to list of lists
            tmp_strat=strat_cursor.fetchall()
            tmp_strat = [list(elem) for elem in tmp_strat]
            tmp_strat = sorted(tmp_strat, key=itemgetter(1))
            
            #loop through the list of orphans
            for idx2,target in enumerate(tmp_target):
                #define set of variables from this particular orphan
                target_sent=target[1]
                target_word=target[2]
                parent = target[4]        
                children = list(sum(eval(target[5]), []))
                words = target[6].split(' ')
                target_id=target[7]
            
                #find all stratigraphic phrases that occur before this orphan and within the defined buffer
                strat_find = [item[1] for item in tmp_strat if target_sent-item[1]<=strat_distance and target_sent-item[1]>0]
                
                #if candidate strat_phrase(s) are found
                if strat_find:
                        #selet the closest sentence with phrase(s)
                        strat_find=max(strat_find)
                        #collect all the strat_phrase(s) in that sentence
                        strat_info = [item for item in tmp_strat if item[1]==strat_find]
                        
                        #define the sentids for sentences that bridge the strat_phrase(s) to the orphan
                        #sent_inbetween=range(strat_find,target[1]+1)
                        sent_inbetween=list(range(strat_find,target[1]+1)), #modified for python 3
                        #collect the words between strat_phrases and orphaned target
                        sent_cursor.execute(""" 
                                    SELECT DISTINCT ON (docid, sentid, words) docid, sentid, words from {NLPname}
                                        WHERE docid=%(my_docid)s
                                        AND   sentid=ANY(%(my_sentid)s);""".format(**dblist),
                                        {
                                          "my_docid": doc[0],
                                          "my_sentid": sent_inbetween
                                            }
                                            )
                        
                        #convert list of tuples to list of lists
                        words_between = sent_cursor.fetchall()
                        words_between = [list(elem) for elem in words_between]
                        words_between = sorted(words_between, key=itemgetter(1))  #不用SQL排序
                        words_between = [' '.join(item[2]) for item in words_between]
                        words_between = ''.join(words_between)
                        words_between = words_between.replace(r"\\' \\'", ",")
                        words_between = words_between.replace(r"\\'", "'")
                        
                        #define the distance between orphan and strat_phrase(s) sentence
                        target_distance = target[1]-strat_find
                        
                        #define grammatical parent and children (as words) of the orphan
                        parent = [words[i] for i in parent]
                        children = [words[i] for i in children]
                       
                        #loop through all the strat_phrases found in the nearest host sentence
                        for match in strat_info:
                            #info about the strat_phrase
                            [docid, sentid, strat_phrase_root, 
                            strat_flag, num_phrase, strat_name_id, 
                            int_name, int_id, age_agree] = match
                           
                            #dump to local variable                
                            #strat_target_distant.append(toadd)
                            #write to psql table
                            cursor.execute(""" 
                                INSERT INTO {strat_target_distant}( docid, 
                                                                 sentid, 
                                                                 strat_phrase_root,
                                                                 strat_flag, 
                                                                 num_phrase, 
                                                                 strat_name_id, 
                                                                 int_name, 
                                                                 int_id,
                                                                 age_agree,
                                                                 target_sent_dist,
                                                                 target_id,
                                                                 target_word,
                                                                 target_parent,
                                                                 target_children,
                                                                 words_between)
                                            VALUES (%s, %s, %s, %s, %s, 
                                                    %s, %s, %s, %s, %s,
                                                    %s, %s, %s, %s, %s);""".format(**dblist),
                                                    (docid, sentid, strat_phrase_root, 
                                                     strat_flag, num_phrase, strat_name_id, 
                                                     int_name, int_id, age_agree, target_distance,
                                                     target_id,target_word,parent,children,
                                                     words_between)
                                                     )
            
        except Exception as e:
            print('\n -------------', "ERROR=",e)
            pass
        
        print("main loop:", (idx+1)/all_, end='\r')
        
    #push the insertions
    connection.commit()
    
    #summary statistic    
    #success = 'number of strat-distant target tuples : %s' %len(strat_target_distant)
    
    #toc
    elapsed_time = time.time() - start_time
    #print ('\n ###########\n\n %s \n elapsed time: %d seconds\n\n ###########\n\n' %(success,elapsed_time))
    print ('\n ########### elapsed time: %d seconds ###########\n\n' %(elapsed_time))
    
    #show a random result
    #r=random.randint(0,len(strat_target_distant)-1); show = "\n".join(str(x) for x in strat_target_distant[r]); 
    #print ("=========================\n" + show +  "\n=========================")
    
    
def alter_strat_target_distant(dblist, credentials):
    print("run alter_strat_target_distant")
    #==============================================================================
    # PROVIDE SUMMARIES FOR AGE-AGREEMENT BETWEEN STRAT_PHRASE AND MACROSTRAT STRAT_NAME
    #==============================================================================
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    

    
    #initialize the age_agree column in strat_phrases
    cursor.execute(""" 
            UPDATE  {strat_target_distant} 
            SET     age_sum = '-';
    """.format(**dblist))
    connection.commit()
    
    #gather distinct Macrostrat links
    cursor.execute("""
        SELECT DISTINCT (strat_name_id) FROM {strat_target_distant};
    """.format(**dblist))
    
    #convert list of tuples to list of lists
    tocheck=cursor.fetchall()
    tocheck = [list(elem) for elem in tocheck]
    
    #find all instances of strat_name_id occuring in the age_check table
    cursor.execute("""
        WITH  query AS(SELECT DISTINCT (strat_name_id) FROM {strat_target_distant})
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
                UPDATE  {strat_target_distant}
                SET     age_sum = %s
                WHERE   strat_name_id = %s;""".format(**dblist),
                (str_counts, strat_name_id)
                )
    connection.commit()
    
    # drop the indexes
    cursor.execute(""" DROP INDEX index_strat_target_distant;""")
    connection.commit()
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
    

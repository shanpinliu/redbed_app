#==============================================================================
#TARGET ADJECTIVE EXTRACTOR
#==============================================================================

# import relevant modules and data
#==============================================================================

# -*- coding: utf-8 -*-

import time, random, re, yaml, psycopg2
from psycopg2.extensions import AsIs

def intial_target_adjectives(dblist, credentials):

    start_time = time.time()
    
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    cursor.execute(""" DROP INDEX IF EXISTS index_NLPname;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_target_instances;""")
    connection.commit()
    
    cursor.execute(""" CREATE INDEX index_NLPname ON {NLPname} (docid, sentid);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_target_instances ON {target_instances} (docid, sentid);""".format(**dblist))
    connection.commit()
    
    #IMPORT TARGETS WITH DEPENDENTS
    cursor.execute("""
        SELECT docid, sentid, target_id, target_word, target_children, target_parent, sentence
        FROM {target_instances}; 
    """.format(**dblist))
    
    target=cursor.fetchall()
    #print(target)
    
    #initalize the target_instances table
    cursor.execute("""
        DELETE FROM {target_adjectives};
    """.format(**dblist))
    
    
    #push drop/create to the database
    connection.commit()
    return target
    
    
def load_target_adjectives(target, shared):
    dblist  = shared[0]
    credentials = shared[1]

    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    all_ = len(target)
    for idx,line in enumerate(target):
        docid, sentid, target_id, target_word, target_children, target_parent, phrase = line
        target_children = eval(target_children)
        target_children = target_children[0]
        
        #IMPORT THE SENTENCES DUMP
        cursor.execute("""
            SELECT docid, sentid, words, poses
            FROM {NLPname} 
            WHERE docid = %(my_docid)s
            AND sentid = %(my_sentid)s; 
            """.format(**dblist), {"my_docid": docid, "my_sentid": sentid})
        
        sentences=cursor.fetchall()
        sentences = [list(elem) for elem in sentences]
        sent = [i for i in sentences[0][2]]
        sent = ' '.join(sent)
        sent = sent.replace(r"\\' \\'", ",")
        sent = sent.replace(r"\\'", "'")
        sent = sent.split(' ')
        children_words = []
        for c in target_children:
            if c < len(sent):
                children_words.append(sent[c])
        
        parent_words = []
        for t in target_parent:
            if t < len(sent):
                parent_words.append(sent[t])
        
        #write to PSQL table
        cursor.execute(""" 
            INSERT INTO {target_adjectives}(   docid,
                                            sentid,
                                            target_id,
                                            target_word,
                                            target_adjective,
                                            target_objective)
            VALUES (%s, %s, %s, %s, %s, %s);""".format(**dblist),
            (docid, sentid, target_id, target_word, children_words, parent_words))
        
        print("main loop:", (idx+1)/all_, end='\r')
    #push insertions to the database
    connection.commit()
    #close the connection
    connection.close()
    

def alter_results(dblist, credentials):
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    
    cursor.execute("""
        ALTER TABLE {results} ADD COLUMN IF NOT EXISTS target_adjective text,
                              ADD COLUMN IF NOT EXISTS target_objective text;
    """.format(**dblist))
    connection.commit()
    
    cursor.execute("""
        UPDATE {results} SET 
        target_adjective = {target_adjectives}.target_adjective, 
        target_objective = {target_adjectives}.target_objective 
        From {target_adjectives}
        WHERE {results}.docid = {target_adjectives}.docid 
        AND {results}.target_id = {target_adjectives}.target_id ;""".format(**dblist))
        
    cursor.execute(""" DROP INDEX index_NLPname;""")
    connection.commit()
    cursor.execute(""" DROP INDEX index_target_instances;""")
    connection.commit()
    
    connection.close()

#==============================================================================
#DEFINE BEGINNING OF REFERENCES SECTION
#==============================================================================

# ACQUIRE RELEVANT MODULES and DATA
#==============================================================================
# -*- coding: utf-8 -*-

import time, psycopg2
import numpy as np



def interpose_refs_location(dblist, credentials, config):
    #tic
    start_time = time.time()
    
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    
    #make some cursors for writing/reading from Postgres
    cursor = connection.cursor()
    doc_cursor=connection.cursor()
    sent_cursor = connection.cursor()
    
    cursor.execute(""" DROP INDEX IF EXISTS index_strat_target;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_strat_target_distant;""")
    connection.commit()
    cursor.execute(""" DROP INDEX IF EXISTS index_NLPname;""")
    connection.commit()
    
    #creat index, this become surprisingly more efficient 
    cursor.execute(""" CREATE INDEX index_strat_target ON {strat_target} (docid);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_strat_target_distant ON {strat_target_distant} (docid);""".format(**dblist))
    connection.commit()
    cursor.execute(""" CREATE INDEX index_NLPname ON {NLPname} (docid, sentid);""".format(**dblist))
    connection.commit()
    
    #==============================================================================
    # FIND REFERENCE SECTIONS
    #==============================================================================
    
    #list of unique docids from target-strat tuples
    doc_cursor.execute("""
        SELECT docid FROM {strat_target}
        UNION
        SELECT docid FROM {strat_target_distant}
    """.format(**dblist))
    
    #initialize Numpy arrays
    refs=np.zeros(0,dtype={'names':['docid','sentid','type','depth'],'formats':['|S100','i4','|S100','f4']})
    best_refs=np.zeros(0,dtype={'names':['docid','sentid','type','depth'],'formats':['|S100','i4','|S100','f4']})

    #loop through documents list
    for idx, doc in enumerate(doc_cursor):
        try:
            #array for reference section for this document
            tmp_refs=np.zeros(0,dtype={'names':['docid','sentid','type','depth'],'formats':['|S100','i4','|S100','f4']})
            
            #collect all sentences for this document
            sent_cursor.execute(""" 
                    SELECT docid, sentid, words from {NLPname}
                        WHERE docid=%(my_docid)s;""".format(**dblist),
                        {
                          "my_docid": doc[0],
                            })
                            
            #loop through sentences
            for idx2, sent in enumerate(sent_cursor):
                docid,sentid,words = sent
                #phrase = ' '.join(words)
                #print(docid,sentid)
                
                """
                docid = docid.replace('–', '-')
                docid = docid.replace('，', ',')
                docid = docid.replace('São', 'Sao')
                docid = docid.replace('‐', '-')
                docid = docid.replace('бп', '')
                docid = docid.replace('б░', '')
                docid = docid.replace('иC', '-')
                docid = docid.replace('—', '-')
                docid = docid.replace('‒', '-')
                docid = docid.replace('δ', '-')
                docid = docid.replace('α', '-')
                docid = docid.replace('бк', '-')
                docid = docid.replace(' ', '-')
                """
                
                try:
                    #REF ID LOGIC: is the first word in a sentence 'References'?
                    if words[0]=='References' or words[0]=='REFERENCES':
                        tmp_refs = np.append(tmp_refs,np.array([(docid,sentid,'ref',0)],dtype=tmp_refs.dtype))
                    
                    #REF ID LOGIC: is the first word in a sentence 'Bibliography'?
                    if words[0]=='Bibliography' or words[0]=='BIBLIOGRAPHY':
                        tmp_refs = np.append(tmp_refs,np.array([(docid,sentid,'ref',0)],dtype=tmp_refs.dtype))
                    
                    #REF ID LOGIC: is the first word in a sentence French for 'Bibliography'?
                    if words[0]=='Bibliographie' or words[0]=='BIBLIOGRAPHIE':
                        tmp_refs = np.append(tmp_refs,np.array([(docid,sentid,'ref',0)],dtype=tmp_refs.dtype))
            
                    #REF ID LOGIC: is there an all capitalized 'REFERENCES' in words array?
                    if 'REFERENCES' in words or 'References' in words:
                        tmp_refs = np.append(tmp_refs,np.array([(docid,sentid,'ref_mention',0)],dtype=tmp_refs.dtype))
                        
                    #REF ID LOGIC: is the word 'Acknowledgements' in words array?
                    if 'Acknowledgements' in words or 'Acknowledgments' in words or 'ACKNOWLEDGEMENTS' in words or 'ACKNOWLEDGMENTS' in words:
                        tmp_refs = np.append(tmp_refs,np.array([(docid,sentid,'ack',0)],dtype=tmp_refs.dtype))
                except:
                    print("docid:", docid)
        
            #null case where no reference section is identified
            if len(tmp_refs)==0:
                tmp_refs = np.array([(docid,0,'none',0)],dtype=tmp_refs.dtype)
            
            #parameter characterizing how deep the reference section is (ref sent #)/(total sent #)
            tmp_refs['depth']=tmp_refs['sentid']/(idx2+1.)    
            
            #all potential reference breaks
            refs = np.append(refs,tmp_refs)
            
            #'Best' reference break is the deepest sentid
            tmp_refs=np.sort(tmp_refs,order='sentid')
            best_refs = np.append(best_refs,tmp_refs[-1])
        except:
            print(doc[0])
            pass
    
    #arbitrary cutoff for 'good' inferences - reset those below threshold to null case
    best_refs['sentid'][best_refs['depth']<0.1]=0
    best_refs['type'][best_refs['depth']<0.1]='none'
    best_refs['depth'][best_refs['depth']<0.1]=0.0
    
    #zeros = best_refs[best_refs['sentid']==0]
    
    #print('best_refs:',best_refs)
    #==============================================================================
    # PUSH REFERENCE FINDINGS TO POSTGRES
    #==============================================================================
    
    #Make a new table
    cursor.execute("""
        DROP TABLE IF EXISTS {refs_location} CASCADE;
        CREATE TABLE {refs_location}(
            docid text,
            sentid int,
            type text,
            depth real);
    """.format(**dblist))
    connection.commit()
    
    #loop through best reference ids and push to Postgres
    for row in best_refs:
        docid=str(row['docid'])[2:-1]
        sentid=str(row['sentid'])
        mtype=row['type']
        depth=str(row['depth'])
                
        cursor.execute("""
        INSERT INTO {refs_location}(docid,
                                    sentid,
                                    type,
                                    depth)
        VALUES (%s, %s, %s, %s);""".format(**dblist),
        (docid,sentid,mtype,depth)
        )
         
    
    #Join reference locations to target-strat tuples
    cursor.execute(""" UPDATE {strat_target}
                            SET refs_loc = {refs_location}.sentid
                            FROM {refs_location}
                            WHERE {strat_target}.docid = {refs_location}.docid""".format(**dblist))
    
    #Join reference locations to target-strat_distant tuples
    cursor.execute(""" UPDATE {strat_target_distant}
                            SET refs_loc = {refs_location}.sentid
                            FROM {refs_location}
                            WHERE {strat_target_distant}.docid = {refs_location}.docid""".format(**dblist))
    
    #Add 'in references'/'out of references' inference to target-strat tuples
    cursor.execute(""" UPDATE {strat_target}
                            SET in_ref = 'yes'
                            WHERE sentid > refs_loc
                            AND   refs_loc <>0""".format(**dblist))
    
    #Add 'in references'/'out of references' inference to target-strat_distant tuples
    cursor.execute(""" UPDATE {strat_target_distant}
                            SET in_ref = 'yes'
                            WHERE sentid > refs_loc
                            AND   refs_loc <>0""".format(**dblist))
    #push changes
    connection.commit()
    
    cursor.execute(""" DROP INDEX index_strat_target_distant;""")
    connection.commit()
    cursor.execute(""" DROP INDEX index_strat_target;""")
    connection.commit()
    cursor.execute(""" DROP INDEX index_NLPname;""")
    connection.commit()
    
    #close the postgres connection
    connection.close()
    
    elapsed_time = time.time() - start_time
    print ('\n ########### elapsed time: %d seconds ###########\n\n' %(elapsed_time))


#%% FOR DEBUGGING

#tmp_refs=best_refs[(best_refs['sentid']!=0)]
#
#tmp = tmp_refs[np.random.choice(len(tmp_refs), 1)]
#
#my_sentid= np.arange(tmp['sentid']-4,tmp['sentid']+20)
#
#sent_cursor.execute(""" 
#        SELECT docid, sentid, words from %(my_app)s_sentences_%(my_product)s
#            WHERE docid=%(my_docid)s
#            AND   sentid = ANY(%(my_sentid)s)
#            ORDER BY sentid;""",
#            {
#              "my_app": AsIs(config['app_name']),
#              "my_product": AsIs(config['product'].lower()),
#              "my_docid": tmp['docid'][0],
#              "my_sentid": (list(my_sentid),)
#                })
#
#phrase=''                
#for idx2, sent in enumerate(sent_cursor):
#    docid,sentid,words = sent
#    words = ' '.join(words)
#    
#    if sentid==tmp['sentid']:
#        flag=words
#        phrase = phrase+'\n*****  '+words
#    else:
#        phrase = phrase+'\n-'+words
##    print words
#    
##    if sentid==tmp['sentid']:
#        
#        
#print '\n ###########\n\n %s \n\n ###########\n\n %s \n\n ###########\n\n' %(phrase,flag)


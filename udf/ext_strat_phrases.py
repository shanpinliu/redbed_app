#==============================================================================
#STRATIGRTAPHIC NAME EXTRACTOR
# ENTITIES = CAPITALIZED WORDS PRECEDING A STRATIGRAPHIC FLAG
# MENTIONS = DEFINED ENTITIES MINUS THE STRATIGRAPHIC FLAG
#
# ENTITY MAPPING DONE ON THE FULL SENTENCES TABLE
# MENTIONS DEFINED BY ENTITIES FOUND IN A GIVEN DOCUMENT
# MENTION MAPPIG DONE ON SENTENCES WITH A TARGET INSTANCE
#==============================================================================

# ACQUIRE RELEVANT MODULES
#==============================================================================

# -*- coding: utf-8 -*-

import time, csv, psycopg2, re, string
from urllib.request import urlopen
from stop_words import get_stop_words
import codecs
import pandas as pd


#==============================================================================
# DEFINE FUNCTION TO DOWNLOAD CSV
#==============================================================================
def download_csv( url ):

    #return variable
    dump_dict = {}

    # get strat_names from Macrostrat API
    # modified for Python 3 compatibility
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


def intial_strat_phrases(dblist, credentials):
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    #initalize the strat_phrases table
    cursor.execute("""
        DELETE FROM {strat_phrases};
    """.format(**dblist))
    connection.commit()
    
    cursor.execute(""" DROP INDEX IF EXISTS index_NLPname;""")
    connection.commit()
    
    cursor.execute(""" CREATE INDEX index_NLPname ON {NLPname} USING Btree (ids);""".format(**dblist))
    connection.commit()
    
    #IMPORT THE SENTENCES DUMP
    cursor.execute("""
        SELECT ids FROM {NLPname};
    """.format(**dblist))
    ids=cursor.fetchall()
    #convert list of tuples to list of lists
    #sentences = [list(elem) for elem in sentences]
    #push drop/create to the database
    connection.commit()
    
    return ids



def load_strat_phrases(ids, shared):
    start_time = time.time()
    
    ids = sorted(ids)
    print("running sentences:", len(ids))
    print(ids[0][0], ids[-1][0])
    dblist  = shared[0]
    credentials = shared[1]
   
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT docid, sentid, words, poses, dep_paths, dep_parents, ids FROM {NLPname} WHERE ids >= %(start)s AND ids <= %(end)s;
    """.format(**dblist), {"start": ids[0][0], "end": ids[-1][0]})
    connection.commit()
    sentences=cursor.fetchall()
    
    #==============================================================================
    # DEFINE STRATIGRPAHIC VARIABLES
    #==============================================================================
    
    #get strat_names from Macrostrat API
    #strat_dict = download_csv( 'https://macrostrat.org/api/defs/strat_names?all&format=csv' )
    strat_dict = ReadCSVtoList("./input/strat_names.csv")
    
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
    period5 = ['to', 'lower', 'upper', 'research', 'just', 'still', 'basin', 'basins']
    stop_words = stop_words + alpha + period1 + period2 + period3 + period4 + period5
    
    special_words = ['de', 'el', 'del', 'la', 'di', 'al']
    # 'late', 'later', 'latest', 'early', 'earlier', 'earliest', 'middle', 'upper', 'lower', 'oldest', 
    
    #STRATIGRAPHIC VARIABLE DEFINITIONS
    #with open('./var/strat_variables.txt') as fid:
        #strat_variables = fid.readlines()
    #for i in strat_variables:
       # exec (i)
    
    #words indicating stratigraphic names
    strat_names = ["Group", "Formation", "Member", "Supergroup", "Subgroup","Gp\.", "Gr\.", "Fm\.", "Mbr\.", "Mb\.", "SGp\.", "Gp", "Gr", "Fm", "Mbr", "Mb", "SGp", "formation", "member"]
    
    # Discard
    #lith_flags = ["Turbidite", "Marlstone", 'Melange', 'Schist', 'Evaporites', 'Psammite', "Tufa", 'Flagstone', "Travertine", "Tar", "Tillite", "Rudstone", "Pelsparite", "Pelmicrite", "Peat", "Packstone", "Oosparite", "Oolite", "Oomicrite", "Micrite", "Coquina", "Novaculite", "Grit", "Intramicrite", "Intrasparite", "Greensand", "Diatomite", "Bindstone", "Biomicrite", "Biosparite", "Boundstone", "Bafflestone", "Anthracite", "Subarkose", "Sublitharenite", "Wacke", "Metasiltstone", "Metapelite", "Siliceous ooze", "Soil", "Lignite", "Pelite", "Phosphorite", "Sand", "Silt", "Bauxite", "Breccia", "Clay", "Diamictite", "Evaporite", "Gravel", "Graywacke", "Gypsum", "Halite", "Iron formation",  "Litharenite", "Mud", "Alluvium", "Anhydrite", "Arenite", "Argillite", "Arkose", "Coal", "Clay", "Ironstone"]
    
    lith_flags = ["Dolomite", "Dolostone", "Limestone","Sandstone", "Shale", "Conglomerate", "Chert", "Mudstone",  "Claystone", "Siltstone", "Carbonate", "Chalk", "Marl", "Marlstone"] #? 
    
    strat_flags = strat_names + lith_flags
    
    #words indicating an age
    age_flags = ["Ga.", "Ga", "Gyr.", "Gyr", "Ma.", "Ma", "Myr.", "Myr", "m.y.", "m.y.r"]
    
    discordance = ['overlain', 'overlying', 'overly', 'overburden', 'underlying', 'underlie', 'underlayer', 'substratum', 'unconformity', 'conformably', 'unconformably', 'overlapped', 'overlies', 'covered', 'covers', 'indistinguishable', 'distinguishable', 'succeeded', 'followed', 'upon'] 
    #==============================================================================
    # LOOK FOR STRATIGRAPHIC NOMENCLATURE  - ENTITY RECOGNITION
    #==============================================================================
    
    #PRE-PROCESS: hack to replace weird strings
    #changed_docs=[];
    
    #initialize the list of found names and list of documents
    #strat_list=[]
    #doc_list={}
    #to_write = []
    
    all_ = len(sentences)
    #loop through sentences
    for idx,line in enumerate(sentences):
        #print("pre line",line)
        line = list(line)
        #print("aft line",line)
        #print("------------------")
        sent = ' '.join(line[2])
        
        
        """
        #有问题！！！！！！！！！！！！！！！！！！！
        for ws in weird_strings:
            if ws[0] in sent:
                #changed_docs.append([line[0], line[1], ws[0], ws[1]])
                #line[2]=[word.replace(ws[0],ws[1]) for word in line[2]]
                sent=sent.replace(ws[0],ws[1])
                
        changed_words_ = sent.split(' ')
    
        changed_words = []
        for i in changed_words_:
            if i != ' ' and len(i)!=0:
                changed_words.append(i)
                
        line[2]=changed_words
                
        line = tuple(line)
        """
        
        #collect individual elements from the psql sentences dump
        doc_id, sent_id, words, poses, dep_paths, dep_parents, ids = line
        
        sent = ' '.join(words)
        sent = sent.replace(r"\\' \\'", ",")
        sent = sent.replace(r"\\'", "'")
        words = sent.split(' ')
        
        #check if there are words of contact relation, which should not be the target strata
        check_dis = [w for w in words if w in discordance]
        p = [pi for pi, p in enumerate(words) if p == '.']
        
        #initialize the variables needed to analyze words in sentence
        i = 0
        #complete_phrase = []
        prior_note = 0
        for word in words:
            i += 1
    
            #initial assumption is a found strat name will have no age information and no link to Macrostrat
            int_name="na"
            int_id='0'
            strat_name_id = '0'
            
            #initialize the lists of word indices and stratigraphic phrase words
            indices=[]
            strat_phrase = []
    
            #logic triggered by discovery of 'stratigraphic' flag (i.e. Formation, etc.)
            if word in strat_flags:
                #record the found word and its index
                indices.append(i)
                this_word = words[i-1]
    
                #initialize variables needed for analysis of preceding words
                preceding_words=[]
                j = 2
                
                #loop to identify preceding stratigraphic modifiers on GOOD_WORD (e.g. Wonoka Formation)
                #loop continues if:
                #   1) the beginning of sentence is not reached
                #   2) the preceding string is not empty
                #   3) the preceding word is not the current word
                #   4) the preceding word is capitalized
                #   5) the preceding capitalized word is not a stratigraphic flag (e.g. Member Wonoka Formation)
                #   6) the preceding word is not a capitalized stop word 
                #   7) the preceding word does not contain a number
                
                # altered, because they forbid a deeper search of age information ahead 
                # (e.g. "-LRB- > 361 -- 313 Ma -RRB- volcanic-sedimentary rocks of the Dahalajunshan Formation")
                while (i-j)>(-1) and len(words[i-j])!=0 and words[i-j] != words[i-j+1] and any([words[i-j][0].isupper(),words[i-j] in special_words]) and not words[i-j][-1].isupper() and words[i-j][-1] not in period4 and words[i-j] not in strat_flags and words[i-j].lower() not in stop_words and re.findall(r'\d+', words[i-j])==[] and words[i-j] not in int_dict['name'] and words[i-j] not in age_flags:  # and words[i-j] not in wrong_words
                #while (i-j)>(-1) and len(words[i-j])!=0 and words[i-j] != words[i-j+1] and words[i-j][0].isupper() and words[i-j] not in strat_flags and words[i-j].lower() not in stop_words and re.findall(r'\d+',  words[i-j])==[]:
                    #loop also broken if preceding word is an interval name (e.g. Ediacaran Wonoka Formation)
                    m=(i-j)
                    for k in range(0,m):
                        if(i-j-k)>prior_note:
                            if  words[i-j-k] in int_dict['name']:
                                int_name=[words[i-j-k]]
                                if(i-j-k-1)>(-1) and words[i-j-k-1].lower() in ['late', 'early', 'middle']:
                                    int_name.insert(0, words[i-j-k-1])
                                    
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
                            
                            elif words[i-j-k] in age_flags:
                                #record age flag with its preceding word (most likely a number)
                                int_name = [words[i-j-k-1], words[i-j-k]]
                                
                                if (i-j-k-1)>0:
                                    if re.findall(r'\d+', words[i-j-k-2])!=[] or words[i-j-k-2] in ['-', '—', '^', 'to', 'and', '±', '+']:
                                        int_name.insert(0, words[i-j-k-2])
                                if (i-j-k-2)>0:
                                    if re.findall(r'\d+', words[i-j-k-3])!=[] or words[i-j-k-3] in ['-', '—', '^', 'to', 'and', '±', '+']:
                                        int_name.insert(0, words[i-j-k-3])
                                    
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
                    preceding_words.append(words[i-j])
                    preceding_text = ' '.join(preceding_words)
                    indices.append((i-j))
                    prior_note = i
                    j += 1
                                     
                if preceding_words and int_name == 'na':
                    next_sect=[n for n,wordn in enumerate(words[i:]) if wordn in strat_flags]
                    if len(next_sect) != 0:
                        m = i + round(min(next_sect)/2)
                        for k in range(i,m):
                            if words[k] in int_dict['name']:
                                int_name=[words[k]]
                                if words[k-1].lower() in ['late', 'early', 'middle']:
                                    int_name.insert(0, words[k-1])
                                    
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
                            
                            elif words[k] in age_flags:
                                #record age flag with its preceding word (most likely a number)
                                int_name = [words[k-1], words[k]]
                                
                                if (k-1)>0:
                                    if re.findall(r'\d+', words[k-2])!=[] or words[k-2] in ['-', '—', '^', 'to', 'and', '±', '+']:
                                        int_name.insert(0, words[i-j-k-2])
                                if (k-2)>0:
                                    if re.findall(r'\d+', words[k-3])!=[] or words[k-3] in ['-', '—', '^', 'to', 'and', '±', '+']:
                                        int_name.insert(0, words[k-3])
                                    
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
                
                #if qualifying preceding words found, join them to the stratigraphic flag and create a stratigraphic phrase
                if preceding_words and len(preceding_words)<5 and len(preceding_text)>2 and len(check_dis) == 0:  # and preceding_text not in wrong_words and preceding_text not in wrong_names
                    #create a full and partial stratigraphic phrase (i.e. with and without the stratigraphic flag)
                    preceding_words.reverse()
                    strat_flag=this_word
                    strat_phrase = ' '.join(preceding_words) + ' ' + this_word
                    strat_phrase_cut = ' '.join(preceding_words)
    
                    #define term to check against Macrostrat's definitions
                    # i.e.  Bitter Springs for Bitter Springs Formation
                    #      Manlius Limestone for Manlius Limestone
                    if strat_flag in lith_flags:
                        strat_phrase_check = strat_phrase
                        
                        #index stratigraphic name to Macrostrat (if present)
                        if strat_phrase_check in strat_dict['strat_name']:
                            #list comprehensions to record strat name id (all string matches regardless of inferred rank)
                            locations2 = [k for k, t in enumerate(strat_dict['strat_name']) if t==strat_phrase_check]
                            loc_ids = [strat_dict['strat_name_id'][L] for L in locations2]
                            if loc_ids:
                                strat_name_id = '~'.join(str(e) for e in loc_ids)
                                
                    else:
                        strat_phrase_check = strat_phrase_cut
    
                        this_word = this_word.replace('Formation', 'Fm')
                        this_word = this_word.replace('formation', 'Fm')
                        this_word = this_word.replace('Fm.', 'Fm')
                        this_word = this_word.replace('Group', 'Gp')
                        this_word = this_word.replace('Gp.', 'Gp')
                        this_word = this_word.replace('Gr.', 'Gp')
                        this_word = this_word.replace('Gr', 'Gp')
                        this_word = this_word.replace('Member', 'Mb')
                        this_word = this_word.replace('member', 'Mb')
                        this_word = this_word.replace('Mbr.', 'Mb')
                        this_word = this_word.replace('Mbr', 'Mb')
                        this_word = this_word.replace('Mb.', 'Mb')
                        this_word = this_word.replace('Supergroup', 'SGp')
                        this_word = this_word.replace('SGp.', 'SGp')
                        this_word = this_word.replace('Subgroup', 'SubGp')
                        
                        strat_phrase = ' '.join(preceding_words) + ' ' + this_word
                        
                        #index stratigraphic name to Macrostrat (if present)
                        if strat_phrase_check in strat_dict['strat_name']:
                            #list comprehensions to record strat name id (all string matches regardless of inferred rank)
                            locations2 = [k for k, t in enumerate(strat_dict['strat_name']) if t==strat_phrase_check]
                            loc_ids = [strat_dict['strat_name_id'][L] for L in locations2 if strat_dict['rank'][L]==this_word]
                            if loc_ids:
                                strat_name_id = '~'.join(str(e) for e in loc_ids)
                    
                    #beginning and end of stratigraphic phrase
                    max_word_id = max(indices)
                    min_word_id = min(indices)
    
                    #create list of stratigraphic phrases found in a given sentence
                    #complete_phrase.append((idx, strat_phrase, strat_phrase_cut, strat_flag, doc_id, sent_id, max_word_id, min_word_id, strat_name_id,int_name,int_id, ' '.join(words)))
    #write to PSQL table  Coal         
                    cursor.execute("""
                        INSERT INTO {strat_phrases}(  docid,
                                                      sentid,
                                                      strat_phrase,
                                                      strat_phrase_root,
                                                      strat_flag,
                                                      phrase_start,
                                                      phrase_end,
                                                      strat_name_id,
                                                      int_name,
                                                      int_id,
                                                      sentence)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(**dblist), (doc_id, sent_id, strat_phrase, strat_phrase_cut, strat_flag, min_word_id, max_word_id, strat_name_id, int_name, int_id, ' '.join(words)))
    
        #once sentence has been mined, add finds to growing list of stratigraphic names
        #for idx,strat_phrase,strat_phrase_cut,strat_flag, doc_id, sent_id, max_word_id, min_word_id, strat_name_id,int_name,int_id, sentence in complete_phrase:
            #dump to local variable
            #strat_list.append('\t'.join([str(x) for x in [idx, doc_id, sent_id, strat_phrase,strat_phrase_cut, strat_flag, min_word_id, max_word_id, strat_name_id,int_name,int_id, sentence]]))
    
            #to_write.append((doc_id, sent_id, strat_phrase,strat_phrase_cut, strat_flag, min_word_id, max_word_id, strat_name_id, int_name, int_id, sentence))
        print("main loop:", (idx+1)/all_, end='\r')
    #push insertions
    connection.commit()
    
    
    #summary statistic
    #success = 'number of stratigraphic entities : %s' %len(strat_list)
    
    #summary of performance time
    elapsed_time = time.time() - start_time
    #print ('\n ###########\n\n %s \n elapsed time: %d seconds\n\n ###########\n\n' %(success,elapsed_time))
    print ('\n\n ########### elapsed time: %d seconds ###########\n\n' %(elapsed_time))
    
    #print out random result
    #r=random.randint(0,len(strat_list)-1); show = "\n".join(str(x) for x in strat_list[r].split('\t')); print ("=========================\n" + show + "\n=========================")



def alter_strat_phrases(dblist, credentials):
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
    
    #initalize the strat_dict table
    cursor.execute("""
        DELETE FROM {strat_dict};
    """.format(**dblist))
    
    cursor.execute("""
        SELECT docid, sentid, strat_phrase, strat_name_id FROM {strat_phrases};
    """.format(**dblist))
    
    #delimiter to separate strat_entities from strat_name_ids in strat_dict
    DICT_DELIM='$$$'
    
    doc_list={}
    for idx,line in enumerate(cursor):
        doc_id, sent_id, strat_phrase, strat_name_id = line
    
        #make dictionary of (strat name, strat_name_id), separated by user defined delimiet, per doc id
        if doc_id in doc_list.keys():
            doc_list[doc_id].add(strat_phrase+DICT_DELIM+strat_name_id)
        else:
            doc_list[doc_id]=set([strat_phrase+DICT_DELIM+strat_name_id])


    #write stratigraphic names found in documents to a PSQL table
    for idx1,doc in enumerate(doc_list.keys()):
        strat_doc = list(doc_list[doc])
        cursor.execute("""
                INSERT INTO {strat_dict}(    docid,
                                           strat_phrase)
                VALUES (%s, %s);""".format(**dblist),
                (doc, strat_doc)
            )
    
    connection.commit()
    
    #some sort of magic
    connection.set_isolation_level(0)
    cursor.execute("""  VACUUM ANALYZE {strat_dict};
    """.format(**dblist))
    connection.commit()
    
    cursor.execute(""" DROP INDEX index_NLPname;""")
    connection.commit()
    
    #close the postgres connection
    connection.close()

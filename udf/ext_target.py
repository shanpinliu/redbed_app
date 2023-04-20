#==============================================================================
#TARGET NAME EXTRACTOR
#==============================================================================

# import relevant modules and data
#==============================================================================

# -*- coding: utf-8 -*-

import time, re, psycopg2


def intial_target_instances(dblist, credentials):
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    #initalize the target_instances table
    cursor.execute("""
        DELETE FROM {target_instances};
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




def load_target_instances(ids, shared):
    start_time = time.time()

    dblist  = shared[0]
    credentials = shared[1]
    ids = sorted(ids)
    print("running sentences:", len(ids))
    print(ids[0][0], ids[-1][0])
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
    
    #initalize list of target occurences
    #target_list=[]
    
    #TARGET DEFINITIONS
    #with open('./var/target_variables.txt') as fid:
    #    target_variables = fid.readlines()
    #for i in target_variables:
    #    exec (i)     #added parentheses for Python 3
    
    bed_names = ['red bed', 'red beds', 'redbed', 'redbeds', 'red-bed', 'red-beds']
    
    color_names = ['red', 'orange', 'purple', 'pink', 'brown', 'yellow', 'green', 'black', 'dark', 'gray', 'grey', 'white', 'blue', 'mottled', 'reddish', 'blackish', 'brownish', 'purplish', 'greenish', 'greyish', 'grayish', 'whitish', 'blueish', 'yellowish', 'pinkish', 'colourless', 'varicolored', 'varicoloured', 'mottled']
    
    position_names = ['basin', 'basins', 'belt', 'belts', 'region', 'regions', 'area', 'areas', 'longitude', 'latitude', 'location', 'located', 'locations', 'coordinates', 'coordinate', 'section', 'sections', 'zone', 'zones', 'field', 'fields']
    
    thickness_names = ['thick', 'thickness']
    
    sed_lith_names = ['conglomerate', 'gravel', 'glutenite', 'breccia', 'metaconglomerate', 'sandstone', 'sand', 'arkose', 'greywacke', 'graywacke', 'greensand', 'arenite', 'subarkose', 'litharenite', 'sublitharenite', 'metagraywacke', 'silicarenite', 'sandy', 'siltstone', 'silt', 'mud', 'mudrock', 'claystone', 'mudstone', 'shale', 'clay', 'soil', 'paleosol', 'argillite', 'marl', 'marlstone', 'pelite', 'metasiltstone', 'metapelite', 'siltite', 'paleosoil', 'muddy', 'silty', 'clayey', 'coal', 'peat', 'carbonate', 'dolomite', 'limestone', 'lime mudstone', 'chalk', 'wackestone', 'packstone', 'grainstone', 'boundstone', 'framestone', 'rudstone', 'bafflestone', 'bindstone', 'dolostone', 'micrite', 'coquina', 'oolite', 'biomicrite', 'oomicrite', 'biosparite', 'pelmicrite', 'oosparite', 'pelsparite', 'intrasparite', 'intramicrite', 'tufa', 'calcarenite', 'marble', 'siliciclastic', 'mixed carbonate-siliciclastic', 'diamictite', 'chert', 'iron formation', 'novaculite', 'laterite', 'tillite', 'wacke', 'flysch', 'slate', 'phyllite', 'quartzite', 'metaquartzite', 'gneiss', 'schist', 'hornfel', 'greenschist', 'metasedimentary', 'paragneiss']
    
    envi_names = ['environment', 'facies'] + ['marine', 'subtidal', 'reef', 'bioherm', 'intrashelf', 'intraplatform', 'platform', 'offshore', 'shelf', 'basinal', 'paralic', 'abyss', 'submarine', 'pelagic', 'deep'] + ['transition', 'transitional', 'estuary', 'tidal', 'shoreline', 'coastal', 'foreshore', 'shoreface', 'peritidal', 'lagoonal', 'deltaic', 'delta', 'prodelta', 'beach', 'shallow'] + ['non-marine', 'eolian', 'loess', 'dune', 'fluvial', 'channel', 'floodplain', 'evaporite', 'levee', 'glacial', 'moraine', 'esker', 'drumlin', 'outwash', 'lacustrine', 'playa', 'alluvial', 'fluvial-lacustrine']
    
    
    target_names = sed_lith_names + bed_names + color_names + envi_names + position_names + thickness_names
    
    
    #an optional list of false hits
    #bad_words = ['outline', 'annotation', 'line', 'dot', 'range', 'centre', 'open', 'symbol', 'box', 'bar', 'curve', 'rim', 'log', 'band', 'polygon', 'star', 'triangle', 'circle', 'spot', 'dashed', 'stipple', 'arrow', 'square', 'rectangle', '=', 'font', 'indicate', 'shading', 'represent', 'nuclei', 'oxide', 'specimen', 'pigment', 'powder', 'stripe', 'core', 'grain', 'moss', 'epoxy', 'crystal', 'fracture', 'residue', 'diamond', 'root', 'spherule', 'patch', 'corpuscle', 'blob', 'refl', 'concretion', 'patina', 'side', 'sideromelane', 'stain', 'tooth', 'pole', 'skin', 'tie', 'asterisk', 'petal', 'mottle', 'vein', 'alga', 'alginite', 'shrub', 'liquid', 'rind', 'plantation', 'spruce', 'shark', 'organism', 'spore', 'material', 'bubble', 'bench', 'molass', 'sea', 'cliff', 'non-reddish', 'ring', 'mark', 'shade', 'neutron', 'pit', 'husk', 'triangles', 'ellipse', 'stripes']
    
    #bad_words = bad_words + ext_mimerals_names
    
    #loop through all sentences. weird_strings
    all_ = len(sentences)
    #to_write = []
    for idx,line in enumerate(sentences):
        #collect individual elements from the psql sentences dump
        docid, sentid, words, poses, dep_paths, dep_parents, ids = line
        #initialize list of local target occurences
        
        #sentence string
        sent = ' '.join(words)
        sent = sent.replace(r"\\' \\'", ",")
        sent = sent.replace(r"\\'", "'")
        words = sent.split(' ')
        
        #b_indices = []
        #for b_name in bad_words:
            #b_matches=[m.start() for m in re.finditer(b_name,sent.lower())]
            #if b_matches:
                #b_indices.extend([sent[0:m].count(' ') for m in b_matches])
        
        #b_indices_ext = []
        #for n in b_indices:
            #b_indices_ext = [n, n-1, n-2, n-3]
        #b_indices_ext=list(set(b_indices_ext))
        #print("b_indices_ext:",b_indices_ext)
        
        
        #loop through all the target names
        for name in target_names:
            #starting index of all matches for a target_name in the joined sentence
    	    matches=[m.start() for m in re.finditer(name,sent.lower())]
            
    	    if matches:
    	        #if at least one match is found, count number of spaces backward to arrive at word index
                indices = [sent[0:m].count(' ') for m in matches]
                
    	        #remove double hits (i.e. stromatolitic-thrombolitic)
                #indices_ = [i for i in indices if i not in b_indices_ext]
                #indices = list(set(indices_))
                indices = list(set(indices))
                #print("indices:",indices)
                 
       	        #target_name spans its starting word index to the number of words in the phrase
                target_word_idx = [[i,i+len(name.split(' '))] for i in indices]
        
       	        #initialize other data about a found target_name
                target_pose=[]
                target_path=[]
                target_parent=[]
        
                for span in target_word_idx:                    
                    #poses, paths and parents can be found at same indices of a target_name find
                    target_word = ' '.join(words[span[0]:span[1]])
                    # extend the word for matching latter
                    target_word_expand = str(' '+target_word+' ')
    
                    # this matching is necessary for short words (e.g. '-red ' is in 'light-red ', but not in 'prepared')          
                    if str(' '+name+' ') in str(target_word_expand) or str('-'+name+'-') in str(target_word_expand) or str('-'+name+' ') in str(target_word_expand) or str(' '+name+'-') in str(target_word_expand):
                        target_children=[]
                        target_pose = poses[span[0]:span[1]]
                        target_path = dep_paths[span[0]:span[1]]
                        target_parent = dep_parents[span[0]:span[1]]
    
                        #children of each component of a target_name
                        for span_idx in range(span[0], span[1]):
                            children = [j for j,i in enumerate(dep_parents) if i==span_idx+1]
                            target_children.append(children)
    
                        #convert parent_ids to Pythonic ids
                        target_parent = [i-1 for i in target_parent]
    
                        #add finds to a local variable
                        #target_list.append([docid, sentid, target_word, span, target_pose, target_path, target_parent, target_children, sent])
    
                        #for easier storage, convert list of target_children lists to a string
                        str_target_children = str(target_children)
    
                        #write to PSQL table
                        #to_write.append((docid, sentid, target_word, span, target_pose, target_path, target_parent, str_target_children, sent))
                         
                        cursor.execute(""" INSERT INTO {target_instances}(    docid,
                                                    				sentid,
                                                    				target_word,
                                                    				target_word_idx,
                                                    				target_pose,
                                                    				target_path,
                                                    				target_parent,
                                                    				target_children,
                                                    				sentence)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""".format(**dblist),
                                                    (docid, sentid, target_word, span, target_pose, target_path, target_parent, str_target_children, sent))
        print("main loop:", (idx+1)/all_, end='\r')
    
    #push insertions to the database
    connection.commit()
    
    #summary statistic
    #success = 'number of target instances: %s' %len(target_list)
    
    #summary of performance time
    elapsed_time = time.time() - start_time
    #print ('\n ###########\n\n %s \n elapsed time: %d seconds\n\n ###########\n\n' %(success,elapsed_time))
    print ('\n\n ########### elapsed time: %d seconds ###########\n\n' %(elapsed_time))
    print(idx)
    #USEFUL BIT OF CODE FOR LOOKING AT RANDOM RESULTS
    #r=random.randint(0,len(target_list)-1); 
    #print ("=========================\n");
    #print (("\n".join(str(target) for target in target_list[r]))); 
    #print ("\n=========================")
    


def alter_target_instances(dblist, credentials):
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    #restart the primary key
    cursor.execute("""
        ALTER TABLE {target_instances} DROP target_id;
    """.format(**dblist))
    
    #push drop/create to the database
    connection.commit()
    
    #add primary key
    cursor.execute(""" ALTER TABLE {target_instances} ADD COLUMN target_id SERIAL PRIMARY KEY;
    """.format(**dblist))
    connection.commit()
    
    #do some magic
    connection.set_isolation_level(0)
    cursor.execute("""  VACUUM ANALYZE {target_instances};
    """.format(**dblist))
    connection.commit()
    
    cursor.execute(""" DROP INDEX index_NLPname;""")
    connection.commit()
    
    #close the connection
    connection.close()
    

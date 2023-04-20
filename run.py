#==============================================================================
#RUN ALL  - STROMATOLITES
#==============================================================================
# coding=utf-8

import time, subprocess, yaml, math

from udf.multiprocessing_tool import processparalle, poolparalle



if __name__ == '__main__':
    #tic
    start_time = time.time()    
    
    #load configuration file
    with open('./config.yml', 'r') as config_yaml:
        config = yaml.load(config_yaml, Loader=yaml.FullLoader)
    
    #load credentials file
    with open('./credentials.yml', 'r') as credential_yaml:
        credentials = yaml.load(credential_yaml, Loader=yaml.FullLoader)    
    
    data_source = r'D:\NLP_results'
    
    #### this is the only variable input  ####
    cores = 50
    
    batch = 'all'
    dblist = {'NLPname':'{app_name}_sentences_nlp352'.format(**config)+'_'+str(batch), 'target_instances':'target_instances'+'_'+str(batch), 
              'target_adjectives':'target_adjectives'+'_'+str(batch), 'strat_phrases':'strat_phrases'+'_'+str(batch), 'strat_dict':'strat_dict'+'_'+str(batch), 
              'strat_target':'strat_target'+'_'+str(batch), 'age_check':'age_check'+'_'+str(batch), 'strat_target_distant':'strat_target_distant'+'_'+str(batch), 
              'bib':'bib', 'results':'results'+'_'+str(batch), 'refs_location':'refs_location'+'_'+str(batch), 'results_new':'results_new'+'_'+str(batch), 
              'output':'output'+'_'+str(batch)}
    
    #INITALIZE THE POSTGRES TABLES
    print ('Step 1: Initialize the PSQL tables ...')
    from setup import run_load_nlp
    run_load_nlp(dblist, credentials, config, data_source)
    
    from udf.initdb import interpose_all
    interpose_all(dblist, credentials)
    
    
    #BUILD THE BIBLIOGRAPHY
    print ('Step 2: Build the bibliography ...')
    from udf.buildbib import interpose_bib
    interpose_bib(dblist, credentials)
    
    
    #FIND TARGET INSTANCES
    print ('\n\n Step 3: Find stromatolite instances ...\n\n ')
    from udf.ext_target import intial_target_instances, load_target_instances, alter_target_instances
    ids = sorted(intial_target_instances(dblist, credentials))
    print("all_sentences:",len(ids))
    cut = 50
    num_data = len(ids)
    offset = math.ceil(num_data/cut)
    for i in range(cut):
        local_min = i*offset
        local_max = (i+1)*offset
        ids_split = ids[local_min:local_max]
        processparalle(ids_split, load_target_instances, cores, shared = [dblist, credentials])
    alter_target_instances(dblist, credentials)
    
    
    #FIND STRATIGRAPHIC ENTITIES
    print ('\n\n Step 4: Find stratigraphic entities ...\n\n ')
    from udf.ext_strat_phrases import intial_strat_phrases, load_strat_phrases, alter_strat_phrases
    ids = sorted(intial_strat_phrases(dblist, credentials))
    print("all_sentences:",len(ids))
    cut = 50
    num_data = len(ids)
    offset = math.ceil(num_data/cut)
    for i in range(cut):
        local_min = i*offset
        local_max = (i+1)*offset
        ids_split = ids[local_min:local_max]
        processparalle(ids_split, load_strat_phrases, cores, shared = [dblist, credentials])
    alter_strat_phrases(dblist, credentials)
    
    
    #FIND STRATIGRAPHIC MENTIONS
    print ('\n\n Step 5: Find stratigraphic mentions ...\n\n ')
    from udf.ext_strat_mentions import intial_strat_mention, load_strat_mention, alter_strat_mention
    doc_list = intial_strat_mention(dblist, credentials)
    processparalle(doc_list, load_strat_mention, cores, shared = [dblist, credentials])
    alter_strat_mention(dblist, credentials)
    
    
    #CHECK AGE - UNIT MATCH AGREEMENT
    print ('\n\n Step 6: Check age - unit match agreement ...\n\n ')
    from udf.ext_age_check import interpose_age_check
    interpose_age_check(dblist, credentials)
    
    #DEFINE RELATIONSHIPS BETWEEN TARGET AND STRATIGRAPHIC NAMES
    print ('\n\n Step 7: Define the relationships between stromatolite phrases and stratigraphic entities/mentions ...\n\n ')
    from udf.ext_strat_target import intial_strat_target, load_strat_target, alter_strat_target
    strat_list = intial_strat_target(dblist, credentials)
    processparalle(strat_list, load_strat_target, cores, shared = [dblist, credentials])
    alter_strat_target(dblist, credentials)
    
    
    #DEFINE RELATIONSHIPS BETWEEN TARGET AND DISTANT STRATIGRAPHIC NAMES
    print ('\n\n Step 8: Define the relationships between stromatolite phrases and distant stratigraphic entities/mentions ...\n\n ')
    from udf.ext_strat_target_distant import intial_strat_target_distant, prepare_strat_target_distant, load_strat_target_distant, alter_strat_target_distant
    intial_strat_target_distant(dblist, credentials)
    docs = prepare_strat_target_distant(dblist, credentials)
    poolparalle(docs, load_strat_target_distant, cores, shared = [dblist, credentials])
    alter_strat_target_distant(dblist, credentials)
    
    
    #FIND BEGINNING OF REFERENCE LIST
    print ('\n\n Step 9: Delineate reference section from main body extractions ...\n\n ')
    from udf.ext_references import interpose_refs_location
    interpose_refs_location(dblist, credentials, config)    

    
    #BUILD A BEST RESULTS TABLE OF STROM-STRAT_NAME TUPLES
    print ('\n\n Step 10: Build a best results table of strom-strat_name tuples ...\n\n ')
    from udf.ext_results import interpose_results
    interpose_results(dblist, credentials, config)
    
    #FIND ADJECTIVES DESCRIBING STROM
    print ('Step 11: Find adjectives describing strom target words ...')
    from udf.ext_target_adjective  import intial_target_adjectives, load_target_adjectives, alter_results
    target = intial_target_adjectives(dblist, credentials)
    processparalle(target, load_target_adjectives, cores, shared = [dblist, credentials])
    alter_results(dblist, credentials)
    
    
    #POSTGRES DUMP
    print ('\n\n Step 12: Dump select results from PSQL ...\n\n ')
    output = 'pg_dump -U '+ credentials['postgres']['user'] + ' -t results -t strat_target -t strat_target_distant -t age_check -t refs_location -t bib -t target_adjectives -t strat_phrases -t target_instancces -d ' + credentials['postgres']['database'] + ' > ./output/{output}.sql'.format(**dblist)
    subprocess.call(output, shell=True)
    
    
    
    
    #summary of performance time
    elapsed_time = time.time() - start_time
    print ('\n ###########\n\n elapsed time: %d seconds\n\n ###########\n\n' %(elapsed_time))

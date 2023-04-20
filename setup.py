import os, re, time, math
import psycopg2

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


def load_nlp(files, dblist, cursor):
    
    squote_strings = ['‘','’',"'"]
    quote_strings = ['"', '“', '”']
    null_strings = ['null','Null', 'NULL', 'nuLL']
    to_write = []
    all_ = len(files)
    for idi, i in enumerate(files):
        data = ReadTxtName(i)
        for idl, line in enumerate(data):
            lines = line.split('\t')
            if len(lines) == 9:
                
                lines3 = lines[3][1:-1].split(',')
                lines6 = lines[6][1:-1].split(',')
                
                lines_3 = []
                lines_6 = []
                
                for qs in quote_strings:
                    if qs in lines[0]:
                        lines[0]=lines[0].replace(qs, '')
                for ss in squote_strings:
                    if ss in lines[0]:
                        lines[0]=lines[0].replace(ss, '')
                        
                for idx, w in enumerate(lines3):
                    for ns in null_strings:
                        if ns in w:
                            string = r'-' + ns + r'-'
                            w=w.replace(ns, string)
                    for qs in quote_strings:
                        if qs in w:
                            w=re.sub(qs, r'\'', w)
                    for ss in squote_strings:
                        if ss in w:
                            w=re.sub(ss, r'\'', w)
                    w = w.replace('{', r'-LCB-')
                    w = w.replace('}', r'-RCB-')
                    w = w.replace('(', r'-LRB-')
                    w = w.replace(')', r'-RRB-')
                    w = w.replace('[', r'-LSB-')
                    w = w.replace(']', r'-RSB-')
                    #w = w.replace('|', '"|"')
                    #w = w.replace('*', r'\*')
                    lines_3.append(w)
                    
                for idx, w in enumerate(lines6):
                    for ns in null_strings:
                        if ns in w:
                            string = r'-' + ns + r'-'
                            w=w.replace(ns, string)
                    for qs in quote_strings:
                        if qs in w:
                            w=re.sub(qs, r'\'', w)
                    for ss in squote_strings:
                        if ss in w:
                            w=re.sub(ss, r'\'', w)
                    w = w.replace('{', r'-LCB-')
                    w = w.replace('}', r'-RCB-')
                    w = w.replace('(', r'-LRB-')
                    w = w.replace(')', r'-RRB-')
                    w = w.replace('[', r'-LSB-')
                    w = w.replace(']', r'-RSB-')
                    #w = w.replace('|', '"|"')
                    #w = w.replace('*', r'\*')
                    lines_6.append(w)
                
                
                lines3_arr = '{' + str({",".join(lines_3)})[2:-2] + '}'
                lines6_arr = '{' + str({",".join(lines_6)})[2:-2] + '}'
                to_write.append((lines[0], lines[1], lines[2], lines3_arr, lines[4], lines[5], lines6_arr, lines[7], lines[8]))

            elif len(lines) != 9:
                print(i,idl)
                print(lines)
        
        print("main loop:", (idi+1)/all_, end='\r')
    
    try:
        cursor.executemany(""" INSERT INTO {NLPname} (docid,sentid,wordidx,words,poses,ners,lemmas,dep_paths,dep_parents) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""".format(**dblist),to_write)
    except psycopg2.Error as e:
        print(e)
        es = str(e)[30:80]
        for item in to_write:
            if es in item[3] or es in item[6]:
                print(item[0], item[1], e)


def run_load_nlp(dblist, credentials, config, data_source):
    #tic
    start_time = time.time()
    
    """Create database and load the sample NLP data"""

    os.environ['PGPASSWORD'] = str(credentials['postgres']['password'])

    # Create the database - if it exists an error will be thrown which can be ignored
    os.system("createdb -h {host} -U {user} -p {port} {database}".format(**credentials['postgres']))

    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()

    # Here we use the config to insert named parameters into the query
    # The ** syntax unpacks a dictionary into keyword arguments to a function
    
    cursor.execute("""
    DROP TABLE IF EXISTS {NLPname}; CREATE TABLE {NLPname} (ids SERIAL primary key, docid text, sentid integer, wordidx integer[], words text[], poses text[], ners text[], lemmas text[], dep_paths text[], dep_parents integer[]);""".format(**dblist))
    cursor.execute("CREATE INDEX ON {NLPname} (docid);".format(**dblist))
    cursor.execute("CREATE INDEX ON {NLPname} (sentid);".format(**dblist))
    
    
    fileList = os.listdir(data_source)
    
    files_path = [os.path.join(data_source, fi) for fi in fileList]
    
    cut = 8
    num_files = len(files_path)
    offset = math.ceil(num_files/cut)
    
    for n in range(cut):
        local_min = n*offset
        local_max = (n+1)*offset
        print('\n\n', local_min, local_max, '\n')
        files = files_path[local_min:local_max]
        load_nlp(files, dblist, cursor)

    connection.commit()
        
    #summary of performance time
    elapsed_time = time.time() - start_time
    print ('###########\t inserting elapsed time: %d seconds\t ###########' %(elapsed_time))

    connection.commit()
    connection.close()


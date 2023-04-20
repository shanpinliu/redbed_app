#==============================================================================
#BUILD A BASIC BIBLIOGRAPHY
#==============================================================================
# -*- coding: utf-8 -*-

import json,psycopg2, yaml

def interpose_bib(dblist, credentials):
    # Connect to Postgres
    connection = psycopg2.connect(
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()
    
    #initialize the table
    cursor.execute("""
        DELETE FROM {bib}
        """.format(**dblist))
    
    connection.commit()
    
    #load in the bibJSON file 
    with open('./input/bibjson', 'r', encoding='utf-8') as fid:
        bib=json.load(fid)
    
    #push docid, authors, title, journal name and url to PostGRES
    for idx,item in enumerate(bib):
    
        #initialize the variables to push to psql
        docid=[]
        title=[]
        journal=[]
        names=[]
        url =[]
    
        #as failsafe, always check if each variable exists
    # JKW updated 'unicode' to 'str' for Python 3 compatibility
    if isinstance(item['_gddid'],str):
        docid=item['_gddid'].encode('ascii','ignore').decode('ascii')
    else:
        docid=item['_gddid']

    if isinstance(item['title'],str):
        title=item['title'].encode('ascii','ignore').decode('ascii')
    else:
        title=item['title']

    if isinstance(item['journal']['name'], str):
        journal=item['journal']['name'].encode('ascii','ignore').decode('ascii')
    else:
        journal=item['journal']['name']

    if 'authors' in item.keys():
        for name in item['authors']:
            names.append(name['name'].encode('ascii','ignore').decode('ascii'))

    if 'link' in item.keys():
        if 'url' in item['link'][0]:
            url=item['link'][0]['url']

            for link in item['link']:
                if link['type']=='sciencedirect':
                    url=link['url']
        #print("-----------------------")
        #print(docid,names,title,journal,url)
        #print(type(names))
        #psql table insertion
        cursor.execute("""
                INSERT INTO {bib} (         docid,
                                          authors,
                                          title,
                                          journal,
                                          url)
                VALUES (%s, %s, %s, %s, %s);""".format(**dblist),
                (docid,names,title,journal,url)
        )
    
    connection.commit()
    
    #update the table with number of instances per journal name
    cursor.execute("""  WITH  query AS(SELECT journal, COUNT(journal)
                                      FROM {bib}
                                      GROUP BY journal)
                        UPDATE {bib}
                            SET journal_instances = query.count
                            FROM query
                            WHERE {bib}.journal = query.journal
    """.format(**dblist))
    connection.commit()
    
    #close the connection
    connection.close()

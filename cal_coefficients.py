# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import mysql.connector
from mysql.connector import MySQLConnection, Error
from mysql.connector import Error
import pandas as pd

def connect():
    try:
        conn = mysql.connector.connect(host='10.0.10.14',database='identitypp_db',user='root',password='')
        if conn.is_connected():
            print('Connected to MySQL database')
    except Error as e:
        print(e)
    finally:
        conn.close()


def query_with_fetchall():
    try:
        #dbconfig = read_db_config()
        dbconfig={'host':'10.0.10.14','database':'identitypp_db','user':'root','password':''}
        conn = MySQLConnection(**dbconfig)
        if conn.is_connected():
            print('Connected to MySQL database')

        cursor = conn.cursor()
        #cursor.callproc('edgeproc')
        cursor.execute("SELECT * FROM identity_edges  order by network_id")
        rows = cursor.fetchall()
        print('Total Row(s):', cursor.rowcount)
        #for row in rows:
        #    print(row)
        return rows

    except Error as e:
        print(e)

    finally:
        cursor.close()
        conn.close()

def cal_coeffs(list_tuple):  #taking argument of tuples
    import numpy as np # linear algebra
    import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
    import matplotlib.pyplot as plt
    #import seaborn as sns
    from sklearn import model_selection, preprocessing
    #import xgboost as xgb
    #color = sns.color_palette()
    #import numpy
    #import pandas 
    from pprint import pprint
    import networkx as nx
    #import matplotlib.pyplot as plt
    import itertools
    import mpld3


   
    #%matplotlib inline
    
    #df=pd.read_csv("C:/Users/Abhishek/Documents/edges.csv")
    df=pd.DataFrame(list_tuple)
    identity_edges_names=['network_id', 'identity_1', 'identity_2', 'edge_linkage_types','edge_linkage_values','edge_level']
    df.columns=identity_edges_names
  
    a=0
    ss=[]    #list of list of edges
    ind_edge=[]
    levels_score={'L':0.10,'M':0.50,'H':0.8,'HM':0.9,'HL':0.85,'LM':0.55,'HH':0.95,'MM':0.6,'LL':0.20,'HLM':0.9,'LMM':0.70,'HHL':0.96,'HMM':0.97,'LLM':0.6}
    levels_var={'ADDR':'H','PHONE':'L','EMAIL':'M','BANK':'H','BUZ':'M','IP':'L'}  #BUZ business name
    linkage_scores=[]
    betw_ck=[]
    avg_c=[]    #will eventually convert to data frame with all the attributes
    linkage_vars=[]
    #levels_cal={}
    #calulating edge strengths, using the dictionaries above
    for i in range(df.shape[0]):
        #print (i)
        if  i<df.shape[0]: #and (i==df.shape[0]-2 or df.network_id[i+1] != df.network_id[i]):
            if df.edge_linkage_types[i].split(','):
                ind_edge.append( df.edge_linkage_types[i].split(',') )
                #print("list")
                #print(ind_edge[-1])
            else:
                ind_edge.append(df.edge_linkage_types[i])
                print("not list")
                print(ind_edge[-1])
            s=[]                                           #to store edges for a single link
            for j in ind_edge[i]:
                s.append('_'.join(levels_var[j]))
                #print(s)
            ss.append(s) #tab mismatch was producing erroneus calaculation
                #print(levels_score[levels_var[i]],levels_var[i],i)
            if len(ss[i])>1:  #if there is more than 1 type of linkahge
                #print(levels_score[''.join(sorted((''.join(ss[i]))))]) 
                #if-else  for handling keys which do not exist in dictionary, passing default 0.5 for now
                if ''.join(sorted((''.join(ss[i])))) in levels_score:
                    linkage_scores.append(levels_score[''.join(sorted((''.join(ss[i]))))])
                    linkage_vars.append(''.join(sorted((''.join(ss[i])))))
                else:
                    linkage_scores.append(0.5)
                    
                
                
            #sorted return list
            else:
                #if-else  for handling keys which do not exist in dictionary, passing default 0.5 for now
                if ''.join(sorted((''.join(ss[i])))) in levels_score:
                    linkage_scores.append(levels_score[''.join(sorted((''.join(ss[i]))))])
                    linkage_vars.append(''.join(sorted((''.join(ss[i])))))
                else:
                    linkage_scores.append(0.5)
                
                
                
                #print (levels_score[(ss[i][0])])
                #linkage_scores.append(levels_score[''.join(sorted((''.join(ss[i]))))])
                #linkage_vars.append(''.join(sorted((''.join(ss[i])))))
                
    df["linkage_scores"]=pd.DataFrame(linkage_scores)    #linkage scores for every row
    df["linkage_vars"]=pd.DataFrame(linkage_vars)        # linkage types for every identity eg'HH','HLM'
    
    
                
    
    linkage_score_mean=[]  
    #influencer_net=[]
    #calculating average_clustering,average linkage score for network
    for i,val in enumerate(df.network_id):#for differnet network ids
        if  i<df.shape[0]-1 and (i==df.shape[0]-2 or df.network_id[i+1] != df.network_id[i]):  #using i+1 so df.shape-1 , change in network id
            edges=list(zip(df.identity_1[a:i+1],df.identity_2[a:i+1]))  # take edges for networkx graph
            #print (edges)
            linkage_score_mean.append([val,df.linkage_scores[a:i+1].mean()])
            #linkage_score_mean=df.linkage_scores[a:i+1].mean()
            link_score=round(float(df.linkage_scores[a:i+1].mean()),2)
            #print ("printing link scores")
            #print (link_score)
            #print (link_score)
            
            a=i+1
            
    
            #networkx
            G=nx.Graph()
            G.add_edges_from(edges)  #for everynetwork id, make graph
            pos = nx.spring_layout(G)
            nx.draw(G,pos)
            print("network %s",(val))
            plt.show() 
#          
            #print(nx.average_clustering(G))
             #print(nx.algorithms.centrality.betweenness_centrality(G))  #betweensess
            betw_ck.append(list(nx.algorithms.centrality.betweenness_centrality(G).items()))
            
            #find influencers based on centrality: dependent on betw_ck
            influencers=[i[0] for i in betw_ck[-1] if i[1]>0.5]
    
            
            #avg clustering coeff ,network density,network size in avg_c,avg  linkage score
            avg_c.append([val,round(nx.average_clustering(G),3),nx.density(G),G.size(),link_score,str(influencers).strip('[]')])
           
            
         
            #print (sorted(betw_ck[-1], key=lambda x: x[1],reverse=True))
            
       
    merged_betw_ck = list(itertools.chain(*betw_ck))      #merging dictionaries  
    return pd.DataFrame(merged_betw_ck),pd.DataFrame(avg_c),df

#update databases
def update(network_to_dict):
    dbconfig={'host':'10.0.10.14','database':'identitypp_db','user':'root','password':''}
    conn = MySQLConnection(**dbconfig)
    if conn.is_connected():
            print('Connected to MySQL database')
    
    cursor = conn.cursor()
    
    # update identity_network tble
    for dic in network_to_dict:
        cursor.execute ("""UPDATE identity_network SET network_cc_score=%s, network_edges_strength=%s, network_influencers=%s 
                        WHERE network_id=%s""", (dic["network_cc_score"],dic["network_edges_strength"],dic["network_influencers"],dic["network_id"]))
    #update identtiy table
    for id in centrality_dict.values.tolist():
        #print(id[0],rouid[1])    
        cursor.execute("""UPDATE identity SET centrality_score=%s WHERE identity_id=%s""", (round(id[1],3),int(id[0])))

    
    conn.commit()
    cursor.close()
    conn.close()
    
    #    for dic in network_to_dict:
#        stmt = "UPDATE identity_network SET "
#        
#        for k,v in dic.items():
#            #print (k,v)
#            #print (k,"",v)
#            if k=='network_density_score' or k=='network_size' or v=='':
#                continue
#            if k=='network_id':
#                n_id=v
#                continue
#            if k=='network_influencers':
#                continue
#                stmt+="%s=' %s'"%(k,v) 
#            else:
#                stmt += "%s = %s, " %(k,v)
#        stmt=stmt[:-2]+" " +"where network_id=%s"%(n_id) +";"  #full staatemnt received
#        query+=stmt
#        print stmt
#    query+=" """  "
#        
#        #' '.join(stmt)
#    
#    for result in cursor.execute(query, multi=True):
#        pass
    
    #cursor.execute(stmt,multi=True)
    #print('##')
    



if __name__ == '__main__':
    #connect()
    r=query_with_fetchall()
    #centrality_dict=id,score  (network_cc=net_id,score) (score_sum=net_id,score)

    centrality_dict,network,df=cal_coeffs(r)
    network.columns=['network_id','network_cc_score','network_density_score','network_size','network_edges_strength','network_influencers']
    identity_edges_names=['network_id', 'identity_1', 'identity_2', 'edge_linkage_types','edge_linkage_values','edge_values']
    network_tuples=list(network.itertuples(index=False))
    
    network_to_dict= network.to_dict(orient='records')
    #update(network_to_dict)
    
    
    r=pd.DataFrame(r)
    r.columns=identity_edges_names
    #print(r)
    #print("printing centrality scores for ids")
    #print(centrality_dict)
    #print ("printing dataframe of network attributes")
    #print(network)
    #score_sum.columns=['net_id','score']
    
    
    
    
    
    

# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 14:52:11 2019

@author: wushansj
"""
from datetime import datetime,timedelta
from elasticsearch import Elasticsearch
import json
import pandas as pd
import requests
import sys

#data = pd.read_csv("D://elk_slack/elk_slack_object.csv") 
data = pd.read_csv("/home/oracle/elk_slack/elk_slack_object.csv") 



LOCAL_TIMEZONE_MINUTES = 8 * 60 
today_datetime = datetime.now()- timedelta(minutes=LOCAL_TIMEZONE_MINUTES)
today_str=str(datetime.strftime(today_datetime,'%Y-%m-%dT%H:%M:%S.%fZ'))
today_30_minutes_ago_str = (today_datetime- timedelta(minutes=180)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
MINIMUM_SHOULD_MATCH = 1

#incomingwebhook
slackurl_rates_detection='https://hooks.slack.com/services/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'
slackurl_api_detection='https://hooks.slack.com/services/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'
slackurl_crawler_detection='https://hooks.slack.com/services/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'

def slack_incoming_webhook(url, text):
    payload={'text': text}
    response=requests.post(url,data=json.dumps(payload),
                           headers={'Content-Type': 'application/json'},verify = False)
    if response.status_code!=200:
        raise ValueError('Request to slack returned an error %s, the response is:\n%s'
                         % (response.status_code, response.text))

def main(argv):

    searchyear = str(today_datetime.year)
    searchmonth = str(today_datetime.month).zfill(2)
    
    es = Elasticsearch("slack:slack_user@linxta-elk01:9200/") 
    indexName = "crawlerlog-"+searchyear+"."+searchmonth
    
    #先取得queryreturn的size,再透過queryBuckets,帶入size,取得all return results
    countquery =  es.search(index=indexName,doc_type='doc', 
                            body={
                                    "aggs": {
                                            "distinct_messages": {
                                                    "cardinality": {
                                                            "script":"doc['server_name.keyword'].value+'||'+\
                                                                      doc['url.keyword'].value+'||'+\
                                                                      doc['datetime_s'].value+'||'+\
                                                                      doc['error_message.keyword'].value"                   
                                                                  }
                                                                 }
                                            },
                                    "query": {
                                            "range":{ 
                                                    "datetime_s":{
                                                            "gte": today_30_minutes_ago_str,
                                                            "lt": today_str
                                                                  }
                                                    }
                                                }
                                })
   
    sizes = countquery['hits']['total']

    if sizes==0:
        buckets=[]
    else:
        queryBuckets = es.search(index=indexName, doc_type="doc",  body={
                "aggs": {
                         "distinct_messages": {
                                                    "terms": {
                                                               "script":"doc['source.keyword'].value+'||'+\
                                                                         doc['url.keyword'].value+'||'+\
                                                                         doc['datetime_s'].value+'||'+\
                                                                         doc['error_message.keyword'].value",                   
                                                               "size": sizes
    
                                                             }
                                                   }
                        },
                "query": {
                        "bool": {
                                "should": [
                                           { 
                                            "range": {
                                                      "datetime_s": 
                                                                   { "gte": today_30_minutes_ago_str,
                                                                     "lt": today_str
                                                                   }
                                                     }
                                            }
    
                                         ],
                                "minimum_should_match": MINIMUM_SHOULD_MATCH
                                }
                        }
                    })
        buckets = queryBuckets['aggregations']['distinct_messages']['buckets']
    
        source=[]
        datetime_s=[]
        error_message=[]
        for index in range(len(buckets)):
            return_str=buckets[index]['key']
            if 'success' not in return_str.split('||')[3]:
                source.append(return_str.split('||')[0])
                datetime_s.append(datetime.strptime(return_str.split('||')[2], '%Y-%m-%dT%H:%M:%S.%fZ')+timedelta(minutes=LOCAL_TIMEZONE_MINUTES))
                error_message.append(return_str.split('||')[3])


        output_df = pd.DataFrame(
            {'source': source,
             'datetime_s': datetime_s,
             'error_message':error_message
            })

        
        output_df_agg = output_df.groupby(['source','error_message']).count()
        output_df_agg.reset_index(inplace=True)  
        output_df_agg = output_df_agg.rename(columns={"datetime_s": "failcnt"})
        merge_df = pd.merge(output_df_agg, data, on = 'source')
        
        merge_df = merge_df.sort_values(by=['owner','source','failcnt'],ascending=False)

        output_api=''
        output_crawler=''
        
        for index, row in merge_df.iterrows():
            if row['type']=='api':
                output_api+='<@%s> <%s|%s> %s failcnt:%d \n' % (row['owner'],row['url'],row['source'],row['error_message'],row['failcnt'])
            if row['type']=='crawler':
                output_crawler+='<@%s> <%s|%s> %s failcnt:%d \n' % (row['owner'],row['url'],row['source'],row['error_message'],row['failcnt'])
        
        
        
        if len(output_api)!=0:
            output_api = '<!channel> \n' + output_api
            slack_incoming_webhook(slackurl_api_detection, output_api)        
        if len(output_crawler)!=0:
            output_crawler = '<!channel> \n' + output_crawler
            slack_incoming_webhook(slackurl_crawler_detection, output_crawler)
        



        
    
if __name__ == "__main__":
    main(sys.argv)

*************
requirement: 
0. install homebrew: http://brew.sh/  || ruby -e "$(curl -fsSL https://raw.github.com/mxcl/homebrew/go)"
1. install python 2.7 : brew install python 2.7
2. install sqlite3 : brew install sqlite3 


*************
data clean:
1. need to narrow down the time into specific time down by limiting tw_created_at in tw_status table. 


**************
data output:
1. tweet_output_txt.py: output tweets in txt file format. can output all or recent o certain number or random of percentage 
2. klout_output_txt.py: output clout score. 


***************
WordScore:
1. https://github.com/jianhuashao/WordScore/wiki
2. https://github.com/jianhuashao/WordScore


*************
100company: data collection on 06 August 2013
1. tw_id are actually company name. twitter only allow to track back latest 3200 tweets, so need to check the data would be missing for event windows. 
2. info chimp data is not available. However, it provides a download database, need to spend time to study on this data file and link the data out. 
3. clout score is update to date on 06 August 2013. 

************
twines: data collection on 26 July 2012
1. data source: http://www.twibes.com/finance/twitter-list
2. clout score is update to date on 06 August 2013. 

************
twines: data collection on 06 August 2013
1. it is not finished at moment

************
ting_analysts: data collection on 06 August 2013
1. tw_id is twitter id here, but klout_id keeps same. 

************
ting_brokers: data collection on 06 August 2013
1. tw_id is twitter id here, but klout_id keeps same. 



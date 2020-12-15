# Projet Python twitter pour determiner les zones climatiquement à risque

import searchtweets
import pandas as pd
import yaml
import time
import matplotlib.pyplot as plt
import geopandas
import descartes

from searchtweets import gen_rule_payload
from searchtweets import ResultStream
from searchtweets import collect_results
from searchtweets import load_credentials


### Credentials

API_key = YOUR_KEY
API_key_secret = YOUR_KEY_SECRET

bearer_token = YOUR_BEARER_TOKEN
access_token = YOUR_ACCESS_TOKEN
access_token_secret = YOUR_ACCESS_TOKEN_SECRET

config = dict(
    search_tweets_prenium = dict(
        account_type = 'premium',
        endpoint = YOUR_ENDPOINT,
        bearer_token = bearer_token,
        consumer_key = YOUR_CONSUMER_KEY,
        consumer_secret = YOUR_CONSUMER_SECRET)
            )
with open('twitter_keys_fullarchive.yaml', 'w') as config_file:
    yaml.dump(config, config_file, default_flow_style=False)

premium_search_args = load_credentials("twitter_keys_fullarchive.yaml",
                                       yaml_key="search_tweets_prenium",
                                       env_overwrite=False)


### Utilisation de twitter et préparation des données


def collect_NWS_tweets_number(number_of_tweet, to_date="2020-12-13"):
    #fonction permettant de collecter un nombre de tweets de @NWS donné ainsi que leur date de publication
    #les dates peuvent être au format "2020-12-13" ou "202012130810" ou "2020-12-13 08:10"
    #number_of_tweet est un int.
    
    rule = gen_rule_payload("from:NWS",
                        from_date="2010-01-01",
                        to_date=to_date,
                        results_per_call=100)
    
    rs = ResultStream(rule_payload=rule,
                  max_results=number_of_tweet,
                  **premium_search_args)
    
    tweets = collect_results(rule, max_results=number_of_tweet, result_stream_args=premium_search_args)
    
    scrapped_tweets = [(tweet.all_text, tweet.created_at_datetime) for tweet in tweets]
    
    return(scrapped_tweets)



def collect_NWS_tweets_date(from_date, to_date="2020-12-13"):
    #fonction permettant de collecter les tweets de @NWS ainsi que leur date de publication sur une période donnée
    #les dates peuvent être au format "2020-12-13" ou "202012130810" ou "2020-12-13 08:10"
    
    text = [] #liste qui contiendra les couples (tweets, date de création)
    last_date = to_date
    step = 900 #on remonte de 900 tweets à chaque itération
    
    while last_date > from_date:
        rule = gen_rule_payload("from:NWS",
                        from_date=from_date,
                        to_date=last_date,
                        results_per_call=100)
    
        rs = ResultStream(rule_payload=rule,
                      max_results=step,
                      **premium_search_args)
    
        tweets = collect_results(rule, max_results=step, result_stream_args=premium_search_args)
    
        scrapped_tweets.append([(tweet.all_text, tweet.created_at_datetime) for tweet in tweets])
        
        last_date = str(scrapped_tweets[-1][1])[:-3] #la date est stockée en datetime, il faut adapter le format pour le passer en arg de gen_rule
        
        time.sleep(61*15) #twitter autorise 900 call toutes les 15 minutes
        
    return(scrapped_tweets)



def list_disaster_type(): 
    #fonction permettant de lister les mots-clés associés aux catastrophes présentes dans la base donnée
    
    disaster_data = pd.read_csv('data/data_base/data_catastrophes.csv', sep=';')
    disaster_subtype = list(set(list(disaster_data['Disaster Subtype'])))
    disaster_subsubtype = list(set(list(disaster_data['Disaster Subsubtype'])))
    disaster_type = list(set(list(disaster_data['Disaster Type'])))
    all_type = disaster_type+disaster_subtype+disaster_subsubtype
    
    all_type = [x.split('(')[0] for x in all_type if isinstance(x,str)] #supprime les élèments non str et ceux contenant des ()

    separate = [] # liste résultant de l'opération : heat/cold' devient 'heat','cold' dans la liste
    for i in range(len(all_type)):
        is_long = True
        while is_long and i<len(all_type):
            disaster = all_type[i]
            if len(disaster.split('/'))>1:
                is_long = True
                all_type = ['']+all_type #permet d'éviter les saut d'éléments de la liste
                all_type.remove(disaster)
                i+=1
                separate = separate+disaster.split('/')
            else:
                is_long= False

    all_type = separate + [type for type in all_type if len(type)] #supprime les '' insérés
    
    return(all_type)


def identify_states(): 
    #la fonction permet d'associer à chaque état de la base de donnée son identifiant
    
    us_cities = pd.read_csv('data/data_base/uscities.csv', sep=',')
    couple_state = list(set([(us_cities['state_name'][i], us_cities['state_id'][i]) for i in range(len(us_cities))])) #ce premier passage permet d'avoir un unique représentant par état
    dic_state = {x[0]:x[1] for x in couple_state}
    
    return(dic_state)


def ready_to_map(scrapped_tweets, disaster_type, dic_state):
    #renvoie deux dictionnaires prêts à être utilisés selon les besoins de mapping
    #scrapped_tweets est le resultat de collect_tweets_date
    #disaster_type est une liste de str représentant les catastrophes naturelles (ex: ['flood','fire'])
    #dic_state est le resultat de identify_states
    
    text_only = [x[0] for x in scrapped_tweets] #ne conserve que le texte de la recherche précédente

    #disaster_tweets repertorie les tweets liés aux catatsrophes naturelles, et l'évenement auxquels ils sont associés
    disaster_tweets = [] #liste contenant les tweets présents dans le dictionnaire
    for disaster in disaster_type:
        for tweet in text_only:
            if disaster.lower() in tweet.lower():
                text_only = ['']+text_only #permet de ne pas sauter d'indice lors du remove
                text_only.remove(tweet) #évite les doublons
                disaster_tweets.append((tweet,disaster)) #on enregistre les tweets et la catastrophe à laquelle ils sont associés
    
    #location_disaster repertorie les types de catstrophes en fonction de la localisation de l'évènement           
    location_disaster = {state: [x[1] for x in disaster_tweets if state.lower() in x[0].lower() or dic_state[state] in x[0].split(' ')] for state in dic_state}
    
    #location_count repertorie le nombre de catastrophe par localisation
    location_count = {state: len(location_disaster[state]) for state in location_disaster}
    
    
    return(location_disaster, location_count)



### Mapping et visualisation

def final_plot_total(scrapped_tweets): 
    #Représente sur une carte le nombre total de catastrophes sur la période souhaitée
    #scrapped_tweets est le resultat de collect_tweets_date
    
    disaster_list = list_disaster_type()
    dic_state = identify_states()
    location_disaster, location_count = ready_to_map(scrapped_tweets, disaster_list, dic_state)
    
    df_count = pd.DataFrame(location_count, index=['Number of catastrophes']).T
    df_count['NAME'] = df_count.index
    df_count = df_count.reset_index(drop=True)
    
    states = geopandas.read_file('data/map/usa-states-census-2014.shp')
    states = states.to_crs("EPSG:3395")
    
    df_plot = pd.merge(states, df_count)
    df_plot.plot(column='Number of catastrophes',cmap='hot', legend=True, figsize=(12, 12))
    plt.title(f"Number of natural disaster {from_date} to {to_date} in the US")
    #plt.savefig(url) où url est le chemin pour enregistrer l'image (ex:r'/Users/Documents/ENSAE_2A/Python_data/map_test.png')


def final_plot_disaster_type(scrapped_tweets, disaster_type): 
    #Représente sur une carte le nombre total d'un type de catastrophe donné sur la période souhaitée
    #scrapped_tweets est le resultat de collect_tweets_date
    #disaster_type est un str. Le type de caractère est indifférent:'FLOOD','FloOd','flood' donneront le même résultat
    
    disaster_list = [disaster_type]
    dic_state = identify_states()
    location_disaster, location_count = ready_to_map(scrapped_tweets, disaster_list, dic_state)
    
    df_count = pd.DataFrame(location_count, index=['Number of catastrophes']).T
    df_count['NAME'] = df_count.index
    df_count = df_count.reset_index(drop=True)
    
    states = geopandas.read_file('data/map/usa-states-census-2014.shp')
    states = states.to_crs("EPSG:3395")
    
    df_plot = pd.merge(states, df_count)
    df_plot.plot(column='Number of catastrophes',cmap='hot', legend=True, figsize=(12, 12))
    plt.title(f"Number of {disaster_type} {from_date} to {to_date} in the US")
    #plt.savefig(url) où url est le chemin pour enregistrer l'image (ex:r'/Users/Documents/ENSAE_2A/Python_data/map_test.png')


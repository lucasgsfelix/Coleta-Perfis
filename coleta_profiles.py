

import pandas as pd

import os

import requests

import time

from multiprocessing import Pool

import re

from functools import partial

import random

import multiprocessing

import requests

import time

import numpy as np

import tqdm

profile_list = open('profile_list.txt', 'a')


def gerar_ip_aleatorio():

	ip = ".".join(str(random.randint(0, 255)) for _ in range(4))
	return ip



def wget_page(link):

	## usar aqui a biblioteca requests
	## verificar como mudar o ip 
	#os.system('wget -q -O bind_address ' + gerar_ip_aleatorio() + ' '
	#		  + link.replace('tripadvisor.com', '').replace('/', '') + ' --directory-prefix=TripPages https://' + link)
	return requests.get("https://" + link)


def cut_page(start_token, end_token, page):
	""" Cut the page.
	Cut the page in the start_token, and then
	the first token that matchs with the position
	bigger than the position of the start token.
	return cut of the page
	"""
	start_pos = [(a.end()) for a in list(re.finditer(start_token, page))]

	if start_pos:
		start_pos = start_pos[0]
		end_pos = [(a.start()) for a in list(re.finditer(end_token, page))]
		end_pos = list(filter(lambda x: x > start_pos, end_pos))[0]

		return page[start_pos:end_pos]

	return 'None'


def retrieve_user_profile(link):

	global profile_list

	page = wget_page(link)	

	profile = cut_page('data-screenName="', '"', page.read())

	unique_id = cut_page('data-memberId="', '"', page.read())

	profile_list.write(unique_id + '\t' + profile + '\t' + link + '\n')

	profile_list.flush()


	os.remove(link.replace('tripadvisor.com', '').replace('/', ''))


if __name__ == '__main__':


	num_cpus = multiprocessing.cpu_count()

	pool = Pool(processes=num_cpus)

	for df in tqdm.tqdm(pd.read_table("profile_links.csv", sep=';', chunksize=1000)):

		print(df)
		
		pool.map(retrieve_user_profile, df['review url crawler'].values)

		time.sleep(np.random.randint(10, 100, size=1)[0])



	profile_list.close()

	pool.close()
	pool.join()

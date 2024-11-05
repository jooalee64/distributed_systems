"""
CPSC 5520, Seattle University
Jooa Lee 
Lab3
"""

from datetime import datetime, timezone, timedelta
import socket
import sys
import threading
import time
import math

import fxp_bytes
import fxp_bytes_subscriber
from bellman_ford import BellmanFord
from collections import defaultdict

import socket
import sys 

# Define subscriber and provider addresses
SUBSCRIBER_ADDRESS = ('127.0.0.1', 50555)  # Replace with actual subscriber address
PROVIDER_ADDRESS = ('127.0.0.1', 50555)    # Replace with actual provider address

class Lab3(object):
	
	def __init__(self, provider_address):
		"""
		:param provider: the address (host, port) tuple of the provider
		"""
		self.provider_address = provider_address
		self.provider_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.graph = {}
		self.last_quotes = {}
		self.last_time = None
		self.provider_socket.bind(SUBSCRIBER_ADDRESS)
		self.running = True

	def incoming_thread(self):
		"""
		incoming thread to receive message
		add quote to create a graph with currencies and exchange rate 
		pass created graph the bellmanford class to detect arbitrage
		"""
		# utc_now = datetime.now(timezone.utc)
		# local_now = datetime.now()
		# last_time = local_now + (utc_now - local_now)
		while True:
			# wait for a message, unmarshal when one is received
			data, addr = self.provider_socket.recvfrom(1024)
			quotes = fxp_bytes_subscriber.demarshal_message(data)

			# print("Received forex quotes:", quotes)
			for quote in quotes:
				formatted_time = quote["time"].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
				# print("formatted_time: ", formatted_time)
				current_time = quote["time"]
				if self.last_time and (current_time < self.last_time):
					print("Ignoring out-of-sequence message")
					continue
					
				self.last_time = current_time
				self.log_quote(current_time, quote["cross"].split()[0], quote["cross"].split()[1], quote["exchange_rate"])
				print(self.add_graph(quote))
				

			bf = BellmanFord(self.graph)
			dist, prev, neg_edge = bf.shortest_paths('USD', 0)
			print("dist:", dist)
			print("prev:", prev)
			print("neg_edge:", neg_edge)
			# if neg_edge:
			# 	self.print_arbitrage(prev, 'USD', init_value=100)
			if neg_edge:
				self.print_arbitrage(prev, start_currency=neg_edge[0], init_value=100)
			
			
		
	def add_graph(self, quote):
		"""
		updates the graph with a new currenies and exchange rate
		:param quote: time, currency1, currency2, exchange_rate
		"""
		curr1, curr2 = quote["cross"].split()
		exchange_rate = quote["exchange_rate"]

		# call to the add_to_graph method to update the graph
		if exchange_rate > 0:
			self.add_to_graph((curr1, curr2), {"timestamp": quote['time'], "price": exchange_rate})
		else:
			print(self.graph)
		
	def add_to_graph(self, currency_pair, quote):
		"""
		adds an exchange rate to the graph for currency pair
		so that we can detect negative edge with rate (weight)

		:param currency_pair: a tuple containig start_currency and end_currency
		:param quote: contains "time, currency1, currency2, exchange_rate" to access rate with key "exchange rate"
		"""
		curr1, curr2 = currency_pair
		rate = -math.log(quote["price"]) if quote["price"] > 0 else 0
		
		if curr1 not in self.graph:
			self.graph[curr1] = {}
		else:
			self.graph[curr1][curr2] = rate 
	
		if curr2 not in self.graph:
			self.graph[curr2] = {}
		else:
			self.graph[curr2][curr1] = -rate
		print(f"Updated graph with edge from {curr1} to {curr2}: {rate:.5f}")


	def print_arbitrage(self, prev, start_currency, init_value=100):
		"""
		Detect and report(log) aribtrage opportunities based on negative cycles
		:param prev: a dictionary mapping with key (predecessor) and values (currencies) in the path 
		:param start_currency: starting currency of arbitrage detection
		:param init_value: default value 
		"""
		steps = []
		last_step = start_currency

		while last_step is not None:
			steps.append(last_step)
			last_step = prev.get(last_step)

		if len(steps) < 2:
			return 
		steps.reverse()
		print("ARBITRAGE:")
		print(f"\tstart with {start_currency} {init_value:.2f}")
		value = init_value
		last = steps[0]

		for i in range(1, len(steps)):
			curr = steps[i]
			price = math.exp(-1 * self.graph[last][curr])
			value *= price
			print(f"\texchange {last} for {curr} at {price} --> {curr} {value:.2f}")
			last = curr 
		profit = value - init_value 
		print(f"\t> profit of {profit:.2f} {start_currency}")

			
	def log_quote(self, timestamp, from_currency, to_currency, exchange_rate):
		"""
		Print the log in the required format: 
		YYYY-MM-DD HH:MM:SS.ssssss from_currency to_currency exchange_rate
		:param 
		"""
		# Format the timestamp as required
		formatted_time = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
		
		# Print in the specified format with exchange rate up to 5 decimal places
		print(f"{formatted_time} {from_currency} {to_currency} {exchange_rate:.5f}")

	
	def subscribe(self):
		"""
		subscriber to provider to listen to price feed messages
		"""
		message = fxp_bytes_subscriber.serialize_address(SUBSCRIBER_ADDRESS)
		self.provider_socket.sendto(message, self.provider_address)
		print("subscription request sent to provider")
		
	
	
		
if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: python lab3.py [provider_host] [provider_port]")
		exit(1)

	subscriber = Lab3(('localhost', 50403))  # Replace with your provider's address
	subscriber.subscribe()
	time.sleep(1.5)
	subscriber.incoming_thread()
	

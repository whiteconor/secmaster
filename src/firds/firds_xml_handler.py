import xml.sax
import json

class FIRDSHandler( xml.sax.ContentHandler ):
	def __init__(self):
		self.in_instrument = False
		self.instrument_list = []
		self.current_instrument = {}
		self.current_context = []

	def instruments(self):
		return self.instrument_list

	# Call when an element starts
	def startElement(self, tag, attributes):
		if tag == 'RefData' or tag == 'FinInstrm':
			self.current_context = []
			self.in_instrument = True
			self.current_instrument = {}
		elif self.in_instrument == True:
			self.current_context.append( tag )

	# Call when an elements ends
	def endElement(self, tag):
		if tag == 'RefData' or tag == 'FinInstrm':
			self.in_instrument = False
			self.instrument_list.append( self.current_instrument )
		elif self.in_instrument == True:
			FIRDSHandler.add_instrument_data( self.current_instrument, self.current_context, self.current_data )
			self.current_data = ''
			self.current_context.pop()

	# Call when a character is read
	def characters(self, content):
		if self.in_instrument == True:
			self.current_data = content
	
	def print(self, output_file ):
		with open( output_file, 'w' ) as jf:
			json.dump( self.instrument_list, jf )

	def add_instrument_data( instrument, context, data ):
		if data.isspace() == False and len(data) > 0:
			pointer = instrument

			for key in context[:-1]:
				if key not in pointer:
					pointer[key] = {}
				pointer = pointer[key]

			pointer[context[-1]] = data

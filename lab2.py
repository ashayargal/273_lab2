import logging
import requests
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from spyne import Application, srpc, ServiceBase, Iterable, UnsignedInteger, \
    String

from spyne.protocol.json import JsonDocument
from spyne.protocol.http import HttpRpc
from spyne.server.wsgi import WsgiApplication


class CrimeReport(ServiceBase):
    @srpc(float, float, float, _returns=Iterable(String))
    def checkcrime(lat, lon, radius):

        url = 'https://api.spotcrime.com/crimes.json?lat=%f&lon=%f&radius=%f&key=.' % (lat, lon, radius)
        print url
        resp = requests.get(url)
        jsnresp = json.loads(resp.content)
        r = jsnresp["crimes"]
        df1 = pd.DataFrame(r)
        x = df1.count()

        """total_crime"""
        totalentries = x[:1].to_json()
        totalentries = json.loads(totalentries)
        total_crime = totalentries["address"]

        """crime_type_count"""
        type1 = df1.loc[:, "type"]
        counttype = type1.value_counts()
        jsntype = counttype.to_json(orient='columns')
        crime_type_count = json.loads(jsntype)

        date = df1['date']
        date = (pd.DataFrame(date.str.split(' ').tolist(), columns=['date', 'time', 'ampm']))
        time = date.loc[:, ['time', 'ampm']]

        time["hour"], time["mins"] = zip(*time["time"].str.split(':').tolist())
        del time['time']


        amtime = time.groupby('ampm').get_group('AM')
        pmtime = time.groupby('ampm').get_group('PM')


        am12_3 = 0
        am3_6 = 0
        am6_9 = 0
        am9_12 = 0
        pm12_3 = 0
        pm3_6 = 0
        pm6_9 = 0
        pm9_12 = 0

        for index, row in amtime.iterrows():

            hour = row['hour']
            hour = int(hour)
            mins = row['mins']
            mins = int(mins)


            if hour < 3 or (hour == 3 and mins == 0) or (hour == 12 and mins > 0):
                am12_3 += 1

            if (6 > hour > 3) or (hour == 6 and mins == 0) or (hour == 3 and mins > 0):
                am3_6 += 1

            if (9 > hour > 6) or (hour == 9 and mins == 0) or (hour == 6 and mins > 0):
                am6_9 += 1

            if 9 < hour < 12 or (hour == 9 and mins > 0):
                am9_12 += 1

            if hour == 12 and mins == 0:
                pm9_12 += 1

        for index, row in pmtime.iterrows():
            hour = row['hour']
            hour = int(hour)
            mins = row['mins']
            mins = int(mins)


            if 3 > hour or (hour == 3 and mins == 0) or (hour == 12 and mins > 0):
                pm12_3 += 1

            if (6 > hour > 3) or (hour == 6 and mins == 0) or (hour == 3 and mins > 0):
                pm3_6 += 1

            if (9 > hour > 6) or (hour == 9 and mins == 0) or (hour == 6 and mins > 0):
                pm6_9 += 1

            if 9 < hour < 12 or (hour == 9 and mins > 0):
                pm9_12 += 1

            if hour == 12 and mins == 0:
                am9_12 += 1

        '''
        print am12_3
        print am3_6
        print am6_9
        print am9_12
        print pm12_3
        print pm3_6
        print pm6_9
        print pm9_12
        '''

        event_time_count = {"12:01am-3am": am12_3, "3:01am-6am": am3_6, "6:01am-9am": am6_9, "9:01am-12noon": am9_12,
                            "12:01pm-3pm": pm12_3, "3:01pm-6pm": pm3_6, "6:01pm-9pm": pm6_9,
                            "9:01pm-12midnight": pm9_12}



        address = {'st':{}}
        for row in r:

            if 'BLOCK BLOCK' in row['address']:
                garbage, street = row['address'].split('BLOCK BLOCK')
                if street in address['st']:
                    address['st'][street] += 1
                else:
                    address['st'][street] = 1

            elif 'BLOCK OF' in row['address']:
                garbage, street = row['address'].split('BLOCK OF')
                if street in address['st']:
                    address['st'][street] += 1
                else:
                    address['st'][street] = 1

            elif 'AND' in row['address']:
                street1, street2 = row['address'].split('AND')
                if street1 in address['st']:
                    address['st'][street1] += 1
                else:
                    address['st'][street1] = 1
                if street2 in address['st']:
                    address['st'][street2] += 1
                else:
                    address['st'][street2] = 1

            elif '&' in row['address']:
                street1, street2 = row['address'].split('&')
                if street1 in address['st']:
                    address['st'][street1] += 1
                else:
                    address['st'][street1] = 1
                if street2 in address['st']:
                    address['st'][street2] += 1
                else:
                    address['st'][street2] = 1

        df2 = pd.DataFrame(address)
        temp = df2.sort_values(by='st', ascending=False)
        temp = temp[:3].to_json()
        print temp
        crime_streets = json.loads(temp)
        crime_streets = crime_streets['st'].keys()




        result = {"total_crime": total_crime, "crime_type_count": crime_type_count, "event_time_count": event_time_count, "the_most_dangerous_streets": crime_streets}
        print (json.dumps(result, sort_keys=True, indent=4))

        yield result






if __name__ == '__main__':
    # Python daemon boilerplate
    from wsgiref.simple_server import make_server

    logging.basicConfig(level=logging.DEBUG)

    # Instantiate the application by giving it:
    #   * The list of services it should wrap,
    #   * A namespace string.
    #   * An input protocol.
    #   * An output protocol.
    application = Application([CrimeReport], 'spyne.examples.hello.http',
                              # The input protocol is set as HttpRpc to make our service easy to
                              # call. Input validation via the 'soft' engine is enabled. (which is
                              # actually the the only validation method for HttpRpc.)
                              in_protocol=HttpRpc(validator='soft'),

                              # The ignore_wrappers parameter to JsonDocument simplifies the reponse
                              # dict by skipping outer response structures that are redundant when
                              # the client knows what object to expect.
                              out_protocol=JsonDocument(ignore_wrappers=True),
                              )

    # Now that we have our application, we must wrap it inside a transport.
    # In this case, we use Spyne's standard Wsgi wrapper. Spyne supports
    # popular Http wrappers like Twisted, Django, Pyramid, etc. as well as
    # a ZeroMQ (REQ/REP) wrapper.
    wsgi_application = WsgiApplication(application)

    # More daemon boilerplate
    server = make_server('127.0.0.1', 8000, wsgi_application)

    logging.info("listening to http://127.0.0.1:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()

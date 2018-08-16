import pandas as pd
from xml.dom import minidom

#DISCLIAMER: NOT COMPLETE

def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.8f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.8f' % (pt['lon']))
        trkseg.appendChild(trkpt)
    
    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)
    
    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)
    
    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')


def get_data(filename):
	#Create the data frame
	parsed = minidom.parse(filename)
	trkpt_list = parsed.getElementsByTagName('trkpt')
	GPS_df = pd.DataFrame(columns=['lat', 'lon'])
	for i in trkpt_list:
		lat = i.attributes['lat'].value
		lon = i.attributes['lon'].value
		GPS_df = GPS_df.append(pd.Series([lat, lon], index=['lat', 'lon']), ignore_index=True)
		
	
def distance(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1- cos((lon2-lon1)*p))/2
    return 12742*asin(sqrt(a))
    


def main():
    points = get_data(sys.argv[1])
    print('Unfiltered distance: %0.2f' % (distance(points),))
    
    smoothed_points = smooth(points)
    print('Filtered distance: %0.2f' % (distance(smoothed_points),))
    output_gpx(smoothed_points, 'out.gpx')


if __name__ == '__main__':
    main()
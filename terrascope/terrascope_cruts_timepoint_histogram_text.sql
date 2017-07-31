/* Copyright (c) 2012-2017 Regents of the University of Minnesota

 This file is part of the Minnesota Population Center's IPUMS Terra Project.
 For copyright and licensing information, see the NOTICE and LICENSE files
 in this project's top-level directory, and also on-line at:
  https://github.com/mnpopcenter/ipums-terra-spatial-functions
*/

-- DROP FUNCTION terrapop_cruts_histograms(big int, text);

CREATE OR REPLACE FUNCTION terrascope_cruts_histograms( cruts_variable_name text, raster_date text) 
RETURNS TABLE (bin_name text, bin_lower double precision, bin_upper double precision, count bigint) AS
	

	$BODY$

	import collections
	import numpy as np



	def GetClimateArray(aVariableName, aDate):

		query = "SELECT netcdf_mnemonic FROM raster_variables WHERE mnemonic = '%s'" % aVariableName
		results = plpy.execute(query)
		plpy.notice(results)
		netcdf_variable = results[0]["netcdf_mnemonic"]
		query = "select %s as values from climate.cruts_322 where time = '%s'" % (netcdf_variable, aDate)
		results = plpy.execute(query)
		allvalues = [r['values'] for r in results]
		newArray = np.array(allvalues)
		
		#Things might break here!!! newArray.min()
		t = np.ma.masked_equal(newArray, newArray.min() )
		#plpy.notice(t.min())
		return (newArray, t.min(), t.max())


	def CreateHistogram(theArray, minValue, maxValue, numBins):
		'''This function creates an ordered dictionary histogram '''
		rangeValue = abs(minValue - maxValue)
		bin_range = float(rangeValue) / numBins
		bin_min = minValue
		theHistogramDictionary = collections.OrderedDict()
		for i in range(0,numBins):
		    bin_name = 'bin_%s' % (i)
		    bin_max = bin_min + bin_range
		    theHistogramDictionary[bin_name] = [bin_min, bin_max, 0]
		    bin_min += bin_range

		for bin_iter in theHistogramDictionary:
		    #print theHistogramDictionary[bin_iter]
		    masked = np.ma.masked_outside(theArray, theHistogramDictionary[bin_iter][0], theHistogramDictionary[bin_iter][1])
		    theHistogramDictionary[bin_iter][2] += masked.count()

		return theHistogramDictionary


	def ConvertHistogramDictionarytoList(theHistogramDictionary):
		package = [[b,theHistogramDictionary[b][0], theHistogramDictionary[b][1], theHistogramDictionary[b][2] ] for b in theHistogramDictionary]

		return package

	


	#cruts_variable_name = 'pre'

	crutsArray, minValue, maxValue = GetClimateArray(cruts_variable_name, raster_date)		
	histogramsDict = CreateHistogram(crutsArray, minValue, maxValue, 50)
	histogramsList = ConvertHistogramDictionarytoList(histogramsDict)


	return histogramsList
    
    $BODY$

LANGUAGE plpythonu VOLATILE;


-- select * FROM terrascope_cruts_histograms( 'CRUTS_DTR', '2013-12-16') 
#Code imports data on NHS clinical commission groups (ccgs) from 2 sources. 2 sources are used as both are incomplete.
#It merges this data with the summaries of the clinical prescriptions made by each ccg.
#This is done file by file to create refined summaries of each file (they are each over 1GB in size), leaving them in a format
#  appropriate for easy, efficient python analysis.
#In hindsight an SQL based approach to manipulating the databases may have been more appropriate. 

from __future__ import division
import numpy as np
import pandas as pd
#import string as str
import glob
import matplotlib
matplotlib.style.use('ggplot')

#Define some constants to be used in loops later
estim = 0
delet = 0

#set path details
path = ""  ###
all_files = glob.glob(path + "/*.CSV")

#Create list to hold filenames
Lpopn = []
ccgcodes = []  

#read population files and ccg data
for f in all_files:
    if '6POPN' in f:
        df = pd.read_csv(f,index_col=False, header=0, usecols=[1,3],names = ['ccgcode','PRACTICE'])
        ccgcodes.append(df)
    elif 'POPN' in f:
        df = pd.read_csv(f,index_col=False, header=0, usecols=[0,1,3,22,23],names = ['ccgname','ccgcode','PRACTICE','popn','QUARTER'])
        df = df.join(df.groupby('ccgcode')['popn'].sum(), on='ccgcode', rsuffix='_r')   #population as per practice, this sums to by ccg in column "popn_r"
        Lpopn.append(df)

#Combine and tidy population files
dfpopn1 = pd.concat(Lpopn)
dfpopn1['popn'] = dfpopn1['popn_r']                                #replace old popn with new, per ccg and delete extra popn column
del dfpopn1['popn_r'],dfpopn1['ccgcode']

#Combine and tidy ccg files
dfccg = pd.concat(ccgcodes)
dfccg = dfccg.dropna()                                             #reads some Na lines which disrupt 
dfccg['ccgcode'] = dfccg['ccgcode'].str[:3]                        #CCG codes as 5 letters (00xxx), 0s removed
dfccg = dfccg.drop_duplicates(subset='PRACTICE',keep='first')      #keep first as leaves most recent in order
dfccg['ccgcode'] = dfccg['ccgcode'].astype(str)                    #data includes minor number of Practice & CCG codes entered wrong that come out//
dfccg['PRACTICE'] = dfccg['PRACTICE'].astype(str)                    #as numbers (removed later), converting all to strings facilitates merge.

#transvect includes each month as "MMYYYY" beside each quarter "QQYYYY" //
  #as popn data is quarterly and pres data monthly, merging pres with transvect then allows merging by correct quarter
transvect = pd.read_csv("/Users/st616/documents/run/transvect2.csv",index_col=None, header=0, names = ['PERIOD','QUARTER'])

for f in all_files:
    if 'PDPI' in f:
        
        #import each monthly file
        print ("Prescription period: %s" %f[28:-9])
        df = pd.read_csv(f,index_col=None, header=0, usecols=[2,3,4,5,7,8,9], names=['PRACTICE','BNF CODE','BNF NAME','ITEMS','ACT COST','QUANTITY','PERIOD'])
        dfpres = df.reindex(np.random.permutation(df.index))
                                    #On cut files usecols = [3,4,5,6,8,9,10], not cut use [2,3,4,5,7,8,9]
                                    
        #remove BNF codes containing no letters - these are bandages and other non-pharmaceuticals   
        dfpres =  dfpres[dfpres['BNF CODE'].str.contains("[a-zA-Z]")]
        print ("Prescription lines: %i" %len(dfpres))

        #assign everything a quarter with the transvect file
        dfpres1 = pd.merge(dfpres,transvect, on='PERIOD',how='left')
    
        #give both dfs a column to join on, combining practice and quarter means pres data labelled with correct population per quarter and practice 
        dfpres1['join'] = dfpres1['PRACTICE'] + dfpres1['QUARTER'].map(str)
        dfpopn1['join'] = dfpopn1['PRACTICE'] + dfpopn1['QUARTER'].map(str)
        
        #Merge everything
        data = pd.merge(dfpres1,dfccg,on='PRACTICE',how='left',copy=False)
        datafin = pd.merge(data,dfpopn1,on='join',how='left')
        print ("Post merge check, prescription lines = %i" % len(datafin))

        #Split any pres data not assigned a population based in it's quarter
        cut = datafin[datafin['popn'].isnull()]
        datafin = datafin[datafin['popn'].notnull()]
        print ("Of which %i were assigned monthly population figures" % len(datafin))
        print ("%i were not." %len(cut)) # for discussion

        #Tidy up and delete old (empty) CCG columns in 'cut' dataframe - being the ones not assigned populations
        del cut['PRACTICE_y'],cut['popn'],cut['QUARTER_y'],cut['ccgname']
        cut['PRACTICE'] = cut['PRACTICE_x']
        datafin['PRACTICE'] = datafin['PRACTICE_x']
        
        #merge 'cut' data with population as at 2011 (best estimate available)
        data2011 = pd.read_csv(path + "/Practice ID.csv", index_col=None, header=0, usecols=[0,2,4], names = ['ccgname','PRACTICE','popn'])
        newcut = pd.merge(cut,data2011,on='PRACTICE',how='left')
        
        #tidy and ensure both dataframes columns align to stick back together
        newcut['practice'] = newcut['PRACTICE']
        datafin['practice'] = datafin['PRACTICE'] 
        del newcut['PRACTICE'],datafin['PRACTICE'],datafin['PRACTICE_y'],datafin['QUARTER_y']
        
        #stick together and add to constant to detail how many prescription lines were estimated populations and how many were deleted (should be 0 deleted)
        final = datafin.append(newcut)  
        print ("of which, all but %i were assigned populations as best estimates" % len(final[final['popn'].notnull()]))
        estim = estim + (len(cut) - len(final[final['popn'].notnull()]))
        delet = delet + len(final[final['popn'].notnull()])
        
        final = final[final['popn'].notnull()]
        print ("%i were removed for having a population of 0" %len(final[final['popn']==0]))    #0s removed as would mess up
        final = final[final['popn']!=0]
        
        #delete ccgcodes that arent ccgcodes and replace 00f, 00g and 00h with 13t as newcastle ccgs combined during timeframe of investigation
        final = final[final['ccgcode'].isin(['00C', '00D', '00F', '00G', '00H', '00J', '00K', '00L', '00M', '00N', '00P', '00Q', '00R', '00T', '00V', '00W', '00X', '00Y', '01A', '01C', '01D', '01E', '01F', '01G', '01H', '01J', '01K', '01M', '01N', '01R', '01T', '01V', '01W', '01X', '01Y', '02A', '02D', '02E', '02F', '02G', '02H', '02M', '02N', '02P', '02Q', '02R', '02T', '02V', '02W', '02X', '02Y', '03A', '03C', '03D', '03E', '03F', '03G', '03H', '03J', '03K', '03L', '03M', '03N', '03Q', '03R', '03T', '03V', '03W', '03X', '03Y', '04C', '04D', '04E', '04F', '04G', '04H', '04J', '04K', '04L', '04M', '04N', '04Q', '04R', '04V', '04X', '04Y', '05A', '05C', '05D', '05F', '05G', '05H', '05J', '05L', '05N', '05P', '05Q', '05R', '05T', '05V', '05W', '05X', '05Y', '06A', '06D', '06F', '06H', '06K', '06L', '06M', '06N', '06P', '06Q', '06T', '06V', '06W', '06Y', '07G', '07H', '07J', '07K', '07L', '07M', '07N', '07P', '07Q', '07R', '07T', '07V', '07W', '07X', '07Y', '08A', '08C', '08D', '08E', '08F', '08G', '08H', '08J', '08K', '08L', '08M', '08N', '08P', '08Q', '08R', '08T', '08V', '08W', '08X', '08Y', '09A', '09C', '09D', '09E', '09F', '09G', '09H', '09J', '09L', '09N', '09P', '09W', '09X', '09Y', '10A', '10C', '10D', '10E', '10G', '10H', '10J', '10K', '10L', '10M', '10N', '10Q', '10R', '10T', '10V', '10W', '10X', '10Y', '11A', '11C', '11D', '11E', '11H', '11J', '11M', '11N', '11T', '11X', '12A', '12D', '12F',  '13P', '13T', '99A', '99C', '99D', '99E', '99F', '99G', '99H', '99J', '99K', '99M', '99N', '99P', '99Q'])]
        final.replace(to_replace=['00F','00G','00H'],value='13T')  
        
        #print out checks to ensure completeness        
        print ("Check - any empty ccg code columns: %i" %len (final[final['ccgcode'].isnull()]))
        print ("final length = %i" %len(final))
        print ("Number of ccg codes in final = %i" %len(final.groupby(['ccgcode'])['ACT COST'].sum()))

        #print to individual csv, naming it as in original file ("mergedMMYYYYPDPI")
        final.to_csv("/Users/st616/documents/output/merged%s.csv" %f[28:-9])
        print ("final length = %i" % len(final))

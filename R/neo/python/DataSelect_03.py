# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 13:02:17 2016

@author: HZ017
"""

def SavePeriods(sD, fD, enum):
    # This function loads the running periods form the Table (ilona's code) and also the data  
    # then it selects long running periods and saves the data in a file
    #
    #The TC washing periods are NOT REMOVED (Data is not filtered)
    import psycopg2
    import datetime
    import numpy
    import scipy.io as sio
    
    # Listing the engines 
    installation = '100041531' #'100041531' '100050648' // 31 for maracanau, 48 for suape
    enum = str(enum)
    if installation == '100041531': #maracanau
        Eng = {'1': '300230659', '2': '300230658', '3': '300230657', '4': '300230656', \
               '5': '300230655', '6': '300230654', '7': '300230650', '8': '300230648' }
        Ch = {'1': '01', '2': '03', '3': '07', '4': '05', '5': '08', '6': '02', '7': '04', '8':'06'}
    else :      #Suape
        Eng = {'1': '300323347', '2': '300323357', '3': '300323351', '4': '300323352', \
               '5': '300323323', '6': '300323324', '7': '300323328', '8': '300323344', \
               '9': '300499587', '10': '300323329', '11': '300323317', '12': '300323321', \
               '13': '300323348', '14': '300323353', '15': '300323349', '16': '300323350', \
               '17': '300323354'}
        Ch = {'1': '01', '2': '02', '3': '03', '4': '04', '5': '05', '6': '06', '7': '07', '8':'08', \
              '9': '09', '10': '10', '11': '11', '12': '12', '13': '13', '14': '14', '15': '15', \
              '16':'16', '17':'17'}

    #connecting to database
    db_url = 'postgresql://postgressadmin:postgressadmin@bender-spock-postgress-db.cnwm9vkygspi.eu-west-1.rds.amazonaws.com:5432/Bender_Spock_Postgress_DB'
    conn = psycopg2.connect(db_url)
        
    # selecting the running periods from the postgress
    if installation == '100041531':
        query = "SELECT * FROM spock.corrected_running_hours_maracanau " + \
            "WHERE mode = 'Running' AND engid = '" + Eng[enum] + "' "
    else:
        query = "SELECT * FROM spock.corrected_running_hours_suapeii " + \
            "WHERE mode = 'Running' AND engid = '" + Eng[enum] + "' "
    cur = conn.cursor()
    cur.execute(query)
    periods = cur.fetchall()
    cur.close() 
    period_start =  numpy.array([t[0].timestamp() for t in periods])
    period_end   =  numpy.array([t[1].timestamp() for t in periods])
    
    # selecting the TCWashing events from the postgress
    query = "SELECT * FROM wois_data.tcevents_detection " + \
            "WHERE event_type = 'TCWash_Rule4' AND engine_id = '" + Eng[enum] + "' " + \
            "ORDER BY ts_end ASC"
    cur = conn.cursor()
    cur.execute(query)
    tc_wash = cur.fetchall()
    cur.close() 
    # getting the end time of the washing events 
    time_wash = numpy.array([t[1].timestamp() for t in tc_wash])
    
    # Fetching the temperature & pressure
    F_T001 = FetchOne('NGA' + Ch[enum] + '1T001PV', conn, sD, fD, Eng[enum])  #00 A bank T inlet
    F_T002 = FetchOne('NGA' + Ch[enum] + '1T002PV', conn, sD, fD, Eng[enum])  #01 B bank T inlet
    F_TE600 = FetchOne('SCA' + Ch[enum] + '1TE600PV', conn, sD, fD, Eng[enum])  #02 T at the TC inlet
    F_TE621 = FetchOne('SCA' + Ch[enum] + '1TE621PV', conn, sD, fD, Eng[enum])  #03 A bank T after compressor
    F_TE631 = FetchOne('SCA' + Ch[enum] + '1TE631PV', conn, sD, fD, Eng[enum])  #04 B bank T after compressor
    F_TE601 = FetchOne('SCA' + Ch[enum] + '1TE601PV', conn, sD, fD, Eng[enum])  #05 T after CAC
    F_PT601 = FetchOne('SCA' + Ch[enum] + '1PT601PV', conn, sD, fD, Eng[enum])  #06 P after CAC
    F_TE511 = FetchOne('SCA' + Ch[enum] + '1TE511PV', conn, sD, fD, Eng[enum])  #07 A bank T before turbine
    F_TE521 = FetchOne('SCA' + Ch[enum] + '1TE521PV', conn, sD, fD, Eng[enum])  #08 B bank T before turbine
    F_TE517 = FetchOne('SCA' + Ch[enum] + '1TE517PV', conn, sD, fD, Eng[enum])  #09 A bank T after turbine
    F_TE527 = FetchOne('SCA' + Ch[enum] + '1TE527PV', conn, sD, fD, Eng[enum])  #10 B bank T after turbine
    F_TAVG = FetchOne('SCA' + Ch[enum] + '1TAVG', conn, sD, fD, Eng[enum])  #11 Average exhaust temperature    
    
    # Fetching the speed and load
    F_SI518 = FetchOne('SCA' + Ch[enum] + '1SI518PV', conn, sD, fD, Eng[enum])     #12 turbine speed A bank
    F_SI528 = FetchOne('SCA' + Ch[enum] + '1SI528PV', conn, sD, fD, Eng[enum])     #13 turbine speed B bank
    F_SI196 = FetchOne('SCA' + Ch[enum] + '1SI196PV', conn, sD, fD, Eng[enum])     #14 Engine speed
    F_UP01 = FetchOne('BAG' + Ch[enum] + '1UP01PV', conn, sD, fD, Eng[enum])       #15 Active power
    F_Q001 = FetchOne('PCA' + Ch[enum] + '1Q001PV', conn, sD, fD, Eng[enum])       #16 Fuel flow
    
    # Others
    F_GT519 = FetchOne('SCA' + Ch[enum] + '1GT519PV', conn, sD, fD, Eng[enum])     #17 Waste gate valve
    F_C001 = FetchOne('SNB' + Ch[enum] + '1C001CV', conn, sD, fD, Eng[enum])       #18 receiver pressure valve
    F_C001CP = FetchOne('SOB' + Ch[enum] + '1C001CPV', conn, sD, fD, Eng[enum])    #19 turbo speed control PV
    F_C001C = FetchOne('SOB' + Ch[enum] + '1C001CV', conn, sD, fD, Eng[enum])      #20 turbo speed control, valve
    F_E001 = FetchOne('NGA901E001PV', conn, sD, fD, '300230659')      #21 Outdoor humidity (absolute, g/kg)    
    F_T001 = FetchOne('NGA901T001PV', conn, sD, fD, '300230659')      #22 Outdoor Temperature, C
    
    #Exhaust T from the cylinders
    F_TE5011A = FetchOne('SCA' + Ch[enum] + '1TE5011APV', conn, sD, fD, Eng[enum])  #23 T A cylinder 1
    F_TE5011B = FetchOne('SCA' + Ch[enum] + '1TE5011BPV', conn, sD, fD, Eng[enum])  #24 T B cylinder 1
    F_TE5021A = FetchOne('SCA' + Ch[enum] + '1TE5021APV', conn, sD, fD, Eng[enum])  #25 T A cylinder 2
    F_TE5021B = FetchOne('SCA' + Ch[enum] + '1TE5021BPV', conn, sD, fD, Eng[enum])  #26 T B cylinder 2
    F_TE5031A = FetchOne('SCA' + Ch[enum] + '1TE5031APV', conn, sD, fD, Eng[enum])  #27 T A cylinder 3
    F_TE5031B = FetchOne('SCA' + Ch[enum] + '1TE5031BPV', conn, sD, fD, Eng[enum])  #28 T B cylinder 3
    F_TE5041A = FetchOne('SCA' + Ch[enum] + '1TE5041APV', conn, sD, fD, Eng[enum])  #29 T A cylinder 4
    F_TE5041B = FetchOne('SCA' + Ch[enum] + '1TE5041BPV', conn, sD, fD, Eng[enum])  #30 T B cylinder 4
    F_TE5051A = FetchOne('SCA' + Ch[enum] + '1TE5051APV', conn, sD, fD, Eng[enum])  #31 T A cylinder 5
    F_TE5051B = FetchOne('SCA' + Ch[enum] + '1TE5051BPV', conn, sD, fD, Eng[enum])  #32 T B cylinder 5
    F_TE5061A = FetchOne('SCA' + Ch[enum] + '1TE5061APV', conn, sD, fD, Eng[enum])  #33 T A cylinder 6
    F_TE5061B = FetchOne('SCA' + Ch[enum] + '1TE5061BPV', conn, sD, fD, Eng[enum])  #34 T B cylinder 6
    F_TE5071A = FetchOne('SCA' + Ch[enum] + '1TE5071APV', conn, sD, fD, Eng[enum])  #35 T A cylinder 7
    F_TE5071B = FetchOne('SCA' + Ch[enum] + '1TE5071BPV', conn, sD, fD, Eng[enum])  #36 T B cylinder 7
    F_TE5081A = FetchOne('SCA' + Ch[enum] + '1TE5081APV', conn, sD, fD, Eng[enum])  #37 T A cylinder 8
    F_TE5081B = FetchOne('SCA' + Ch[enum] + '1TE5081BPV', conn, sD, fD, Eng[enum])  #38 T B cylinder 8
    F_TE5091A = FetchOne('SCA' + Ch[enum] + '1TE5091APV', conn, sD, fD, Eng[enum])  #39 T A cylinder 9
    F_TE5091B = FetchOne('SCA' + Ch[enum] + '1TE5091BPV', conn, sD, fD, Eng[enum])  #40 T B cylinder 9
    F_TE5101A = FetchOne('SCA' + Ch[enum] + '1TE5101APV', conn, sD, fD, Eng[enum])  #41 T A cylinder 10
    F_TE5101B = FetchOne('SCA' + Ch[enum] + '1TE5101BPV', conn, sD, fD, Eng[enum])  #42 T B cylinder 10

    conn.close()    
    
    #preparing the time grid
    t0 = datetime.datetime.strptime(str(sD), "%Y%m%d")
    t0 = t0.timestamp()
    t1 = datetime.datetime.strptime(str(fD), "%Y%m%d")
    t1 = t1.timestamp() + 24*3600    #take the end of the day
    T = numpy.arange(t0, t1, 3600*0.01)
    
    #creating the LIST containing the data
    DataList = {}
    count = 0
    for i in range(len(periods)):
        #define the grid
        ts = max(period_start[i] + 5*3600, t0)  #shift the time according to the zones?
        tf = min(period_end[i] + 5*3600, t1)  #shift the time according to the zones?
        tt = [t for t in T if (t >= ts + 2*3600) & (t <= tf - 2*3600)]   
              
        # Adding data    
        if (tf > ts + 6*3600):
            # Calculating the running time since last washing at the beggining of the period
            ind = numpy.where(time_wash < tt[0])[0]
            if len(ind) == 0:
                t_sincelast = 0  #time since the last TC washing at the beggining of the period
            else:
                t_last = time_wash[ind[-1]]
                dp = numpy.array( [min(tt[0], t) for t in period_end] ) - \
                     numpy.array( [max(t_last, t) for t in period_start] )
                t_sincelast = dp[dp>0].sum() #time since the last TC washing at the beggining of the period
    
            #Calculating the time since the last washing
            t_sincelast = tt - tt[0]  + t_sincelast            
            ind = numpy.where( (time_wash > tt[0]) & (time_wash < tt[-1]) )[0]
            for j in ind:
                tw = time_wash[j]
                v = tt - tw
                t_sincelast[tt > tw] = numpy.minimum(t_sincelast[tt > tw], v[tt>tw] )
                
            #Recording the data    
            data = numpy.array([ F_T001(tt), F_T002(tt), F_TE600(tt), F_TE621(tt), F_TE631(tt), F_TE601(tt),  \
                    F_PT601(tt), F_TE511(tt), F_TE521(tt), F_TE517(tt), F_TE527(tt), F_TAVG(tt), \
                    F_SI518(tt), F_SI528(tt), F_SI196(tt), F_UP01(tt), F_Q001(tt), \
                    F_GT519(tt), F_C001(tt), F_C001CP(tt), F_C001C(tt), F_E001(tt), F_T001(tt), \
                    F_TE5011A(tt), F_TE5011B(tt), F_TE5021A(tt), F_TE5021B(tt), \
                    F_TE5031A(tt), F_TE5031B(tt), F_TE5041A(tt), F_TE5041B(tt), \
                    F_TE5051A(tt), F_TE5051B(tt), F_TE5061A(tt), F_TE5061B(tt), \
                    F_TE5071A(tt), F_TE5071B(tt), F_TE5081A(tt), F_TE5081B(tt), \
                    F_TE5091A(tt), F_TE5091B(tt), F_TE5101A(tt), F_TE5101B(tt), t_sincelast ])
            DataList.update({str(count): data})        
            DataList.update({str(count) + 't' : tt})
            count = count + 1
    sio.savemat("Engine" + enum, DataList)
         
    
def DrawPeriod(i, enum):
    #Drawing
    import scipy.io as sio
    import matplotlib.pyplot as plt
    DataList = sio.loadmat("Engine" + str(enum) )
    i = str(i)
    T = DataList[i + 't'][0]
    T = (T - T[0])/3600
         
    fig1 = plt.figure()
    plt.subplot(411)        #Temperature before engine
    plt.plot(T, DataList[i][0], 'b')
    plt.plot(T, DataList[i][1], 'b.')
    plt.plot(T, DataList[i][3], 'g')
    plt.plot(T, DataList[i][4], 'r')
    plt.plot(T, DataList[i][5], 'k')
    plt.axis([T[0], T[-1], 0, 200]) #plt.axis([0, T, 400, 600])
    plt.legend(['T inlet, A bank', 'T inlet, B bank', \
                'T after compressor, A bank', 'T after compressor, B bank', \
                'T before engine'])
    
    plt.subplot(412)        #Temperature after engine
    plt.plot(T, DataList[i][7], 'b')
    plt.plot(T, DataList[i][8], 'g')
    plt.plot(T, DataList[i][9], 'r')
    plt.plot(T, DataList[i][10], 'k')
    plt.plot([T[-1], T[-1]], [450, 450], 'm--')
    plt.axis([T[0], T[-1], 0, 600]) #plt.axis([0, T, 400, 600])
    plt.legend(['T before turbine, A bank', 'T before turbine, B bank', \
                'T after turbine, A bank', 'T after turbine, B bank'])
    
    plt.subplot(413)  
    plt.plot(T, DataList[i][12]/15000, 'b')
    plt.plot(T, DataList[i][13]/15000, 'g')
    plt.plot(T, DataList[i][15]/20000, 'r')  #active power
    plt.plot(T, DataList[i][14]/600, 'k')   #engine speed
    plt.axis([T[0], T[-1], 0, 1.1]) # plt.axis([0, T, 500, 700])
    plt.legend(['Turbine speed, A bank', 'Turbine speed, B bank', \
                'Active power', 'Engine speed'])
           
    plt.subplot(414) 
    plt.plot(T, DataList[i][43]/3600, 'b')
    plt.axis([T[0], T[-1], 0, 300]) # plt.axis([0, T, 500, 700])
    plt.legend('Time since last washing')
    
    
def FetchOne(VarName, conn, sD, fD, Estr):
    #fetching 1 variable
    import datetime
    from scipy.interpolate import interp1d        
    cur = conn.cursor()
    query= "SELECT * FROM wois_data.time_series_data " \
            "WHERE tag = '" + VarName + "' AND engine_id = '" + Estr + "' " \
            "AND ts >= '" + str(sD) + " 00:00:00' AND ts <= '" + str(fD) + " 23:59:59' " \
            "ORDER BY ts ASC"
    print("Your query is: " + query)
    cur.execute(query)
    data = cur.fetchall()
    cur.close() 
               
    #converting data into lists
    t0 = datetime.datetime.strptime(str(sD), "%Y%m%d")
    t0 = t0.timestamp()
    t1 = datetime.datetime.strptime(str(fD), "%Y%m%d")
    t1 = t1.timestamp()
    v1t = [];
    v1v = [];
    for i in data:
        dt = i[0].timestamp()
        v1t.append( dt )  #converting seconds to hours
        v1v.append(i[1])
        
    v1t = [t0] + v1t + [t1 + 24*3600]
    v1v = [v1v[0]] + v1v + [v1v[-1]]
    return interp1d(v1t, v1v)
    

# -*- coding: utf-8 -*-
"""
Created on Mon Oct 24 13:02:17 2016

@author: HZ016
"""
#This is the function to fill in the database

def Wash_Record(enum):
    import matplotlib.pyplot as plt
    import psycopg2
    import datetime
    import numpy
    
    # Starting and ending dates (not anymore needed, but let it be)
    # must cover all available data, 
    #otherwise can be problems to detect events at the begging and at the end
    sD = 20110101
    fD = 20151231
    
    #connecting to database
    db_url = 'postgresql://postgressadmin:postgressadmin@bender-spock-postgress-db.cnwm9vkygspi.eu-west-1.rds.amazonaws.com:5432/Bender_Spock_Postgress_DB'
    conn = psycopg2.connect(db_url)
    
#    # deleting old records
#    cur = conn.cursor()
#    cur.execute("DELETE FROM wois_data.events WHERE event_type = 'TCWash_Rule3' ")
#    cur.close()
#    conn.commit()
    
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
    

    # Fetching the temperature & pressure
    F_TE511 = FetchOne('SCA' + Ch[enum] + '1TE511PV', conn, sD, fD, Eng[enum])  # A bank T before turbine
    F_TE521 = FetchOne('SCA' + Ch[enum] + '1TE521PV', conn, sD, fD, Eng[enum])  # B bank T before turbine
    F_TE517 = FetchOne('SCA' + Ch[enum] + '1TE517PV', conn, sD, fD, Eng[enum])  # A bank T after turbine
    F_TE527 = FetchOne('SCA' + Ch[enum] + '1TE527PV', conn, sD, fD, Eng[enum])  # B bank T after turbine

    # fetching the speed and load
    F_SI518 = FetchOne('SCA' + Ch[enum] + '1SI518PV', conn, sD, fD, Eng[enum])  #turbine speed A bank
    F_SI528 = FetchOne('SCA' + Ch[enum] + '1SI528PV', conn, sD, fD, Eng[enum])  #turbine speed B bank
    F_SI196 = FetchOne('SCA' + Ch[enum] + '1SI196PV', conn, sD, fD, Eng[enum])  #Engine speed
    F_UP01 = FetchOne('BAG' + Ch[enum] + '1UP01PV', conn, sD, fD, Eng[enum])  #Active power

    #fetching the TC wash data
    F_TR001 = FetchOne('NHC' + Ch[enum] + '1TR001PV', conn, sD, fD, Eng[enum])  #Time to turbine wash
        
    
    #preparing the time grid
    t0 = datetime.datetime.strptime(str(sD), "%Y%m%d")
    t0 = t0.timestamp()
    t1 = datetime.datetime.strptime(str(fD), "%Y%m%d")
    t1 = t1.timestamp() 
    T = numpy.arange(0, (t1-t0)/3600 + 24, 0.01)

    # Pre-calculations fo the modified index
    min_511_prev = numpy.array( F_TE511(T) )
    min_521_prev = numpy.array( F_TE521(T) )
    min_D1_prev = numpy.array( F_TE511(T)-F_TE517(T) )
    min_D2_prev = numpy.array( F_TE521(T)-F_TE527(T) )
    min_D1_next = numpy.array( F_TE511(T)-F_TE517(T) )
    min_D2_next = numpy.array( F_TE521(T)-F_TE527(T) )
    min_speed_prev = numpy.array( F_SI196(T) )         
    min_speed_next = numpy.array( F_SI196(T) ) 
    for i in range(1, 51):
        min_511_prev[i:] = numpy.minimum(min_511_prev[i:], F_TE511( T[:-i]) )
        min_521_prev[i:]  = numpy.minimum(min_521_prev[i:], F_TE521(T[:-i]) )
        min_D1_prev[i:] = numpy.minimum(min_D1_prev[i:], F_TE511(T[:-i]) - F_TE517(T[:-i]) )
        min_D2_prev[i:] = numpy.minimum(min_D2_prev[i:], F_TE521(T[:-i]) - F_TE527(T[:-i]) )
        min_D1_next[:-i] = numpy.minimum(min_D1_next[:-i], F_TE511(T[i:]) - F_TE517(T[i:])   )
        min_D2_next[:-i] = numpy.minimum(min_D2_next[:-i], F_TE521(T[i:]) - F_TE527(T[i:])   )
        min_speed_prev[i:] = numpy.minimum(min_speed_prev[i:], F_SI196( T[:-i]) )
        min_speed_next[:-i] = numpy.minimum(min_speed_next[:-i], F_SI196(T[i:]) )
        
    #Calculating the modified and the original Flags         
    WashFlag = (F_TE517(T) < F_TE511(T) - 150) & ( F_TE527(T) < F_TE521(T) - 150) & \
               (F_UP01(T) < 7000) & (F_SI196(T) >= 300)  & \
               (F_SI518(T) < 8000) & (F_SI528(T) < 8000) & \
               (min_511_prev < 460) & (min_521_prev < 460) & \
               (F_TE511(T) - F_TE517(T) > min_D1_prev + 100) & \
               (F_TE521(T) - F_TE527(T) > min_D2_prev + 100) & \
               (F_TE511(T) - F_TE517(T) > min_D1_next + 100) & \
               (F_TE521(T) - F_TE527(T) > min_D2_next + 100) & \
               ( numpy.logical_or(min_speed_prev > 300, min_speed_next > 300) )   
    WashFlag = WashFlag*1           

    #Clustering to events
    d_plus  = numpy.where(WashFlag[1:] - WashFlag[:-1] == 1)[0]
    d_minus = numpy.where(WashFlag[1:] - WashFlag[:-1] == -1)[0]
    if (len(d_plus) > 0) & (len(d_minus) > 0):
        if d_plus[0] > d_minus[0] :       #event at the beggning, not added
            d_minus = d_minus[1:]
            print('Dropping out the first minus')            
        if d_minus[-1] < d_plus[-1] :     #event at the end
            d_plus = d_plus[:-1]            
            print('Dropping out the last plus')
        #check that the events are separated (at least 30 min between the events)            
        c = numpy.where( d_plus[1:] - d_minus[:-1] < 50 )[0]
        d_plus = numpy.delete(d_plus, numpy.array(c)+1)
        d_minus = numpy.delete(d_minus, c)
    
        #Adding events ot the table 
        cur = conn.cursor()
        for i in range(len(d_plus)):
            dp = d_plus[i]      #event start time
            dm = d_minus[i]     #event end time
            dT1 = max(F_TE511(T[dp:dm])) - min(F_TE517(T[dp:dm]))
            dT2 = max(F_TE521(T[dp:dm])) - min(F_TE527(T[dp:dm]))
            sT = datetime.datetime.fromtimestamp(t0 + 3600*T[dp] )            
            fT = datetime.datetime.fromtimestamp(t0 + 3600*T[dm] ) 
            cur.execute("INSERT INTO wois_data.events " + \
                        "(ts_start, ts_end, installation_id, engine_id, event_type, dt) " + \
                        "VALUES (%s, %s, %s, %s, %s, %s)", \
                        (sT, fT, installation, str(Eng[enum]), 'TCWash_Rule4', max(dT1, dT2)) )
        cur.close()
        conn.commit()
        
    #Closing the connection to the database
    conn.close()               

   
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
        v1t.append( (dt-t0)/3600 )  #converting seconds to hours
        v1v.append(i[1])
        
    v1t = [0] + v1t + [(t1-t0)/3600 +24]
    v1v = [v1v[0]] + v1v + [v1v[-1]]
    return interp1d(v1t, v1v)

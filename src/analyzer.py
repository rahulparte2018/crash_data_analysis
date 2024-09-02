from pyspark.sql.functions import col, count, countDistinct, row_number, monotonically_increasing_id,sum
from pyspark.sql.window import Window

# Define the list of state names and abbreviations
states_list = [
    ("Alabama", "AL"),
    ("Alaska", "AK"),
    ("Arizona", "AZ"),
    ("Arkansas", "AR"),
    ("California", "CA"),
    ("Colorado", "CO"),
    ("Connecticut", "CT"),
    ("Delaware", "DE"),
    ("District of Columbia", "DC"),
    ("Florida", "FL"),
    ("Georgia", "GA"),
    ("Hawaii", "HI"),
    ("Idaho", "ID"),
    ("Illinois", "IL"),
    ("Indiana", "IN"),
    ("Iowa", "IA"),
    ("Kansas", "KS"),
    ("Kentucky", "KY"),
    ("Louisiana", "LA"),
    ("Maine", "ME"),
    ("Maryland", "MD"),
    ("Massachusetts", "MA"),
    ("Michigan", "MI"),
    ("Minnesota", "MN"),
    ("Mississippi", "MS"),
    ("Missouri", "MO"),
    ("Montana", "MT"),
    ("Nebraska", "NE"),
    ("Nevada", "NV"),
    ("New Hampshire", "NH"),
    ("New Jersey", "NJ"),
    ("New Mexico", "NM"),
    ("New York", "NY"),
    ("North Carolina", "NC"),
    ("North Dakota", "ND"),
    ("Ohio", "OH"),
    ("Oklahoma", "OK"),
    ("Oregon", "OR"),
    ("Pennsylvania", "PA"),
    ("Rhode Island", "RI"),
    ("South Carolina", "SC"),
    ("South Dakota", "SD"),
    ("Tennessee", "TN"),
    ("Texas", "TX"),
    ("Utah", "UT"),
    ("Vermont", "VT"),
    ("Virginia", "VA"),
    ("Washington", "WA"),
    ("West Virginia", "WV"),
    ("Wisconsin", "WI"),
    ("Wyoming", "WY"),
    ("Mexico", "MX")
]

# Convert the list to a dictionary
states_dict = dict(states_list)

class Analyzer:
    def __init__(self, loader, saver) -> None:
        self.loader = loader
        self.saver = saver

    def analyze(self, code):
        # print('Anlysis code received is', code)
        if code == 0:
            self.__all_analysis()
            print(f"Successfully Completed All Analysis")
            return

        analysis_dict = {
            1: self.__analysis_for_code_1,
            2: self.__analysis_for_code_2,
            3: self.__analysis_for_code_3,
            4: self.__analysis_for_code_4,
            5: self.__analysis_for_code_5,
            6: self.__analysis_for_code_6,
            7: self.__analysis_for_code_7,
            8: self.__analysis_for_code_8,
            9: self.__analysis_for_code_9,
            10: self.__analysis_for_code_10,
        }
        if code in analysis_dict:
            analysis = analysis_dict[code]
            result = analysis()
            print(f"Successfully Completed Analysis {code}")
            return result
        else:
            return

    def __analysis_for_code_1(self):
        print('Performing Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?')
        df = self.loader.load_table('primary_person')
        # df.show(5)

        filtered_df = df.filter( (df['PRSN_INJRY_SEV_ID'] == 'KILLED') & (df['PRSN_GNDR_ID'] == 'MALE') ).select('CRASH_ID')
        # print(f'filter rows = {filtered_df.count()}')
        
        result = filtered_df.groupBy('CRASH_ID').agg(count('*').alias('cnt')).filter( col('cnt')>2 )
        
        number_of_crashes = result.count()
        result = f'The number of crashes (accidents) in which number of males killed are greater than 2 are {number_of_crashes}'
        print(result) 
        return result
    
    def __analysis_for_code_2(self):
        print('Performing Analysis 2: How many two wheelers are booked for crashes?')
        df = self.loader.load_table('units')
        filtered_df = df.filter( df['VEH_BODY_STYL_ID'] == 'MOTORCYCLE' ).select('CRASH_ID')
        # result = filtered_df.select('CRASH_ID').distinct().count()
        result = filtered_df.count()
        result = f'Number of two wheeler booked for crashes are {result}'
        print(result)
        return result
    
    def __analysis_for_code_3(self):
        print('Performing Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.')
        df1 = self.loader.load_table('primary_person')
        filtered_df1 = df1.filter( (df1['PRSN_TYPE_ID']=='DRIVER') & (df1['PRSN_INJRY_SEV_ID']=='KILLED') & (df1['PRSN_AIRBAG_ID']=='NOT DEPLOYED')).select('CRASH_ID','UNIT_NBR')
        # print(filtered_df1.count())
        body_style_list = [
            'NEV-NEIGHBORHOOD ELECTRIC VEHICLE',
            'OTHER  (EXPLAIN IN NARRATIVE)',
            'PASSENGER CAR, 2-DOOR',
            'PASSENGER CAR, 4-DOOR',
            'PICKUP',
            'POLICE CAR/TRUCK',
            'SPORT UTILITY VEHICLE',
            'TRUCK',
            'UNKNOWN',
            'VAN'
        ]
        df2 = self.loader.load_table('units').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_BODY_STYL_ID')
        filtered_df2 = df2.filter( ~(df2['VEH_MAKE_ID'].isin(['NA','UNKNOWN','OTHER (EXPLAIN IN NARRATIVE)','ALL OTHER MAKES'])) & (df2['VEH_BODY_STYL_ID'].isin(body_style_list)))
        # print(filtered_df2.count())
        join_df3 = filtered_df2.join(filtered_df1, on=['CRASH_ID','UNIT_NBR'], how='inner')
        result = join_df3.groupBy('VEH_MAKE_ID').agg(count('*').alias('cnt')).orderBy(col('cnt').desc()).drop('cnt').limit(5)
        # result.show()
        return result
      
    def __analysis_for_code_4(self):
        print('Performing Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?')
        df1 = self.loader.load_table('primary_person')
        lic_type_list = ['COMMERCIAL DRIVER LIC.', 'DRIVER LICENSE', 'OCCUPATIONAL', 'OTHER']
        filtered_df1 = df1.filter( (df1['PRSN_TYPE_ID']=='DRIVER') & (df1['DRVR_LIC_TYPE_ID'].isin(lic_type_list)) & ~(df1['DRVR_LIC_CLS_ID']=='UNLICENSED') )
        # print(filtered_df1.count())

        df2 = self.loader.load_table('units')
        filtered_df2 = df2.filter( (df2['VEH_HNR_FL']=='Y') )
        # print(filtered_df2.count())

        join_df3 = filtered_df2.join(filtered_df1, on=['CRASH_ID','UNIT_NBR'], how='inner')
        result = join_df3.count()
        ans = f"Number of Vehicles with driver having valid licences involved in hit and run are {result}"
        print(ans)
        return ans
    
    def __analysis_for_code_5(self):
        print('Performing Analysis 5: Which state has highest number of accidents in which females are not involved?')
        df = self.loader.load_table('primary_person')

        females_group_df = df.filter( (df['PRSN_GNDR_ID']=='FEMALE') ).select('CRASH_ID').distinct()
        # print(females_group_df.count())

        groups_without_females_df = df.select('CRASH_ID').distinct().subtract(females_group_df)
        # print(groups_without_females_df.count())

        crash_list = [ row['CRASH_ID'] for row in groups_without_females_df.collect() ]
        # print(crash_list[:10], type(crash_list))

        result = df.filter( (df['CRASH_ID'].isin(crash_list)) & ~(df['DRVR_LIC_STATE_ID'].isin(['Unknown','NA'])) ).groupBy('DRVR_LIC_STATE_ID').agg( countDistinct('CRASH_ID').alias('cnt') ).orderBy( col('cnt').desc() ).limit(10)
        # result.show()
        return result
    
    def __analysis_for_code_6(self):
        print('Performing Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death')
        df = self.loader.load_table('units')
        
        result = df.withColumn('final_injr_cnt', col('UNKN_INJRY_CNT') + col('TOT_INJRY_CNT') + col('DEATH_CNT') ).groupBy('VEH_MAKE_ID').agg( sum('final_injr_cnt').alias('cnt') )
        # result.show(10)

        result = result.orderBy(col('cnt').desc()).withColumn('row_index', monotonically_increasing_id()).filter( col('row_index').between(3,5) ).select('VEH_MAKE_ID')

        # result.show()
        return result
    
    def __analysis_for_code_7(self):
        print('Performing Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style')
        df1 = self.loader.load_table('primary_person').select('CRASH_ID','UNIT_NBR','PRSN_ETHNICITY_ID').filter( ~(col('PRSN_ETHNICITY_ID').isin(['NA','OTHER','UNKNOWN'])))
        df2 = self.loader.load_table('units').select('CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID').filter( ~(col('VEH_BODY_STYL_ID').isin(['NA','NOT REPORTED','UNKNOWN','OTHER  (EXPLAIN IN NARRATIVE)'])) )
        df3 = df1.join(df2, on=['CRASH_ID','UNIT_NBR'], how='inner')
        df_grouped = df3.groupBy(['VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID']).agg(count('*').alias('cnt'))
        window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('cnt').desc())
        df_ranked = df_grouped.withColumn('rank', row_number().over(window_spec))
        result = df_ranked.filter( col('rank')==1 ).drop('rank').drop('cnt')
        # result.show(truncate=False)
        return result
    
    def __analysis_for_code_8(self):
        print('Performing Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)')
        factor_list = ['HAD BEEN DRINKING','UNDER INFLUENCE - ALCOHOL']
        car_type_list = ['NEV-NEIGHBORHOOD ELECTRIC VEHICLE','PASSENGER CAR, 2-DOOR','PASSENGER CAR, 4-DOOR','PICKUP','POLICE CAR/TRUCK','SPORT UTILITY VEHICLE','TRUCK','VAN']
        df1 = self.loader.load_table('units').select('CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID','CONTRIB_FACTR_1_ID','CONTRIB_FACTR_2_ID','CONTRIB_FACTR_P1_ID')\
            .filter( ((col('CONTRIB_FACTR_1_ID').isin(factor_list)) | (col('CONTRIB_FACTR_2_ID').isin(factor_list)) | (col('CONTRIB_FACTR_P1_ID').isin(factor_list))) & (col('VEH_BODY_STYL_ID').isin(car_type_list)) )
        # df1.show(10, truncate=False)
        df2 = self.loader.load_table('primary_person').select('CRASH_ID','UNIT_NBR','DRVR_ZIP').filter( (col('DRVR_ZIP').isNotNull()) )
        df_joined = df1.join(df2, on=['CRASH_ID','UNIT_NBR'], how='inner')
        df_grouped = df_joined.groupBy('DRVR_ZIP').agg(countDistinct('CRASH_ID').alias('cnt'))
        df_ordered = df_grouped.orderBy(col('cnt').desc())
        # df_ordered.show(10,truncate=False)
        result = df_ordered.drop('cnt').limit(5)
        # result.show(truncate=False)
        return result
    
    def __analysis_for_code_9(self):
        print('Performing Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance')
        damage_levels = ['DAMAGED 5','DAMAGED 6','DAMAGED 7 HIGHEST']
        df1 = self.loader.load_table('units').select('CRASH_ID','UNIT_NBR','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID','FIN_RESP_TYPE_ID') \
                .filter( ((col('VEH_DMAG_SCL_1_ID').isin(damage_levels)) | (col('VEH_DMAG_SCL_2_ID').isin(damage_levels))) & (col('FIN_RESP_TYPE_ID').contains('INSURANCE')) )
        
        df2 = self.loader.load_table('damages').select('CRASH_ID')

        all_crashes = df1.select('CRASH_ID').distinct()
        property_damaged_crashes = df2.select('CRASH_ID').distinct()
        required_crashes = all_crashes.subtract(property_damaged_crashes)
        result = required_crashes.count()
        # print(result, all_crashes.count(), property_damaged_crashes.count(), required_crashes.count())
        result = f'The Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance is {result}'
        print(result)
        return result
    
    def __analysis_for_code_10(self):
        print('Performing Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)')
        df1 = self.loader.load_table('units')
        df2 = self.loader.load_table('primary_person')
        df3 = self.loader.load_table('charges')

        color_df = df1.filter( ~(col('VEH_COLOR_ID')=='NA') ).groupBy('VEH_COLOR_ID').agg(count('*').alias('cnt')).orderBy(col('cnt').desc()).limit(10)
        top_colors = [row['VEH_COLOR_ID'] for row in color_df.collect()]
        # print(top_colors)

        states_df = df2.filter( ~(col('DRVR_LIC_STATE_ID').isin(['NA','UNKNOWN','OTHER','Unknown','Other'])) ).groupBy('DRVR_LIC_STATE_ID').agg(countDistinct('CRASH_ID').alias('cnt')).orderBy(col('cnt').desc()).limit(25)
        top_states = [row['DRVR_LIC_STATE_ID'] for row in states_df.collect()]
        # print(top_states)

        top_states_code = [states_dict[key] for key in top_states]
        # print(top_states_code)

        charged_df = df3.filter( (col('CHARGE').contains('SPEED')) ).select('CRASH_ID','UNIT_NBR')
        # print(charged_df.show(5, truncate=False))
        # print(charged_df.count())

        lic_type_list = ['COMMERCIAL DRIVER LIC.', 'DRIVER LICENSE', 'OCCUPATIONAL', 'OTHER']
        driver_with_valid_license = df2.filter( (col('PRSN_TYPE_ID')=='DRIVER') & (col('DRVR_LIC_TYPE_ID').isin(lic_type_list)) & ~(col('DRVR_LIC_CLS_ID')=='UNLICENSED') ).select('CRASH_ID','UNIT_NBR')
        # print(driver_with_valid_license.count())

        licensed_charged_driver = charged_df.join(driver_with_valid_license, on=['CRASH_ID','UNIT_NBR'], how='inner')
        # print(licensed_charged_driver.count())

        filtered_df = df1.select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_COLOR_ID','VEH_LIC_STATE_ID').filter( (col('VEH_COLOR_ID').isin(top_colors)) & (col('VEH_LIC_STATE_ID').isin(top_states_code)))
        # print(filtered_df.count())

        joined_df = filtered_df.join(licensed_charged_driver, on=['CRASH_ID','UNIT_NBR'], how='inner')
        result = joined_df.groupBy('VEH_MAKE_ID').agg(count('*').alias('cnt')).orderBy(col('cnt').desc()).drop('cnt').limit(5)
        # result.show(truncate=False)

        return result
    
    def __all_analysis(self):
        self.saver.save_results(self.__analysis_for_code_1(), f"Analytics_report_1")
        self.saver.save_results(self.__analysis_for_code_2(), f"Analytics_report_2")
        self.saver.save_results(self.__analysis_for_code_3(), f"Analytics_report_3")
        self.saver.save_results(self.__analysis_for_code_4(), f"Analytics_report_4")
        self.saver.save_results(self.__analysis_for_code_5(), f"Analytics_report_5")
        self.saver.save_results(self.__analysis_for_code_6(), f"Analytics_report_6")
        self.saver.save_results(self.__analysis_for_code_7(), f"Analytics_report_7")
        self.saver.save_results(self.__analysis_for_code_8(), f"Analytics_report_8")
        self.saver.save_results(self.__analysis_for_code_9(), f"Analytics_report_9")
        self.saver.save_results(self.__analysis_for_code_10(), f"Analytics_report_10")

        
    

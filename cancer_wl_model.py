import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time
import xlsxwriter

t0=time.time()
################################################################################
                    #####Data Read and Pre-Process#####
################################################################################
sdmart_engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
                           'trusted_connection=yes&driver=ODBC+Driver+17'\
                               '+for+SQL+Server')

####Waitlist Additions
add_sql = """ SELECT [WkEnd_Added] AS [Week End]
		  ,[Specialty] AS [Specialty Code]
          ,[Clinic Code] AS [Clinic Code]
		  ,[Priority]
		  ,[New/Follow-Up] AS [New/Follow Up]
		  ,SUM([W/List Additions]) AS [Waitlist Additions]
		  FROM [SDMartDataLive2].[infodb].[PowerBI].[RL_PBI0043_WL_Adds]
		  WHERE WkEnd_Added < GETDATE()
		  GROUP BY [WkEnd_Added], [Specialty], [Clinic Code], [Priority], [New/Follow-Up]"""
add = pd.read_sql(add_sql, sdmart_engine)

####Waitlist Attendances
att_sql = """SELECT [Session Week] as [Week End]
				 ,[Specialty] AS [Specialty Code] 
                 ,[clinic_code] AS [Clinic Code]
				 ,[prity_perf] as [Priority]
				 ,CASE WHEN [visit_desc] = 'FU' THEN 'Follow Up' else [visit_desc] END AS [New/Follow Up]   			 
				 ,SUM([Attended]) AS [Attendances]
				 FROM [infodb].[PowerBI].[RL_PBI0043_Activity]
				 WHERE rundate = (select MAX(rundate) from [infodb].[PowerBI].[RL_PBI0043_Activity]) AND [Session Week] < GETDATE()
				 GROUP BY [Session Week], [Specialty], [clinic_code], [prity_perf], [visit_desc]"""
att = pd.read_sql(att_sql, sdmart_engine)

####Historical Waitlist Size
wl_sql = """SELECT CONVERT(DATE, [Session Week]) AS [Week End]
				,[Specialty] AS [Specialty Code]
                ,[clinic_code] AS [Clinic Code]       
				,[Priority]        
				,[New/Follow Up]
				,SUM([Waitlist Size]) AS [Waitlist Size]
				FROM [infodb].[PowerBI].[RL_PBI0043_WL_Past]
				WHERE  [Session Week] <  GETDATE()
				GROUP BY [Session Week], [Specialty], [clinic_code], [Priority], [New/Follow Up]
"""
wl = pd.read_sql(wl_sql, sdmart_engine)
wl['Week End'] = wl['Week End'].astype('datetime64[ns]')

####Specialty Lookup
pfmgt_spec_sql = """SELECT spcd AS [Specialty Code],
                           pfmgt_spec,
                           pfmgt_spec_desc AS [Specialty]
                           FROM infodb.dbo.vw_cset_specialties"""
pfmgt_spec = pd.read_sql(pfmgt_spec_sql, sdmart_engine)

####Join together
#Correct date formats between past and future in output, not the same format
cancer_wl = wl.merge(
                add.merge(att, on=['Week End', 'Specialty Code', 'Clinic Code',
                                   'Priority', 'New/Follow Up'], how='outer'),
                on=['Week End', 'Specialty Code', 'Clinic Code', 'Priority',
                    'New/Follow Up'], how='outer')
#Merge onto specialty descriptions, ensure no unwanted specialties are included
cancer_wl['Specialty Code'] = cancer_wl['Specialty Code'].str.strip()
cancer_wl = cancer_wl.merge(pfmgt_spec, on='Specialty Code', how='left')
cancer_wl = cancer_wl.loc[~cancer_wl['Specialty Code'].isin(['ZZ','ZN','99'])]

####Futre slots
cancer_slots_sql = """--SLOTS
SELECT
DATEADD(DAY, 7 - (@@DATEFIRST-1) - DATEPART(WEEKDAY, infodb.dbo.fn_remove_time(util.[session_start_dttm])), 
		infodb.dbo.fn_remove_time(util.[session_start_dttm])) AS [Week End]
,spec.[pfmgt_spec_desc] AS [Specialty Name]
,spec.[pfmgt_spec] AS [Specialty]
,util.[clinic_code] AS [Clinic Code]
,CASE WHEN util.[new_fup_status] ='N' THEN 'New'
	  WHEN util.[new_fup_status] ='F' THEN 'Follow Up'
	  WHEN util.[new_fup_status] ='U' THEN 'Undefined'
	  ELSE util.[new_fup_status] END AS [New/Follow Up]
,SUM(util.[available_temp]) AS [Slots] --proper capacity 

FROM  infodb.dbo.[vw_sess_util] AS util

LEFT JOIN infodb.dbo.[vw_cset_specialties] AS spec
          ON util.[spect_refno] = spec.[spect_refno]  -- Specialties table
LEFT JOIN PiMSMarts.dbo.[MasterClinicList] AS mastc
		  ON util.[spont_refno] = mastc.[spont_refno]

WHERE
--Filter to slots over next 3 weeks
(YEAR(util.[session_start_dttm]) * 100 + DATEPART(WEEK, util.[session_start_dttm])) 
BETWEEN 
  (YEAR(GETDATE()) * 100 + DATEPART(WEEK, GETDATE())) --Start of current week
   AND
  (YEAR(DATEADD(WEEK, 3, GETDATE())) * 100 + DATEPART(WEEK, DATEADD(WEEK, 2, GETDATE()))) --End of 3 weeks time
--Other filterings
AND util.[session_cancr_dttm] IS NULL -- Restrict to held sessions only
AND util.[template_flag] ='N' --not templates
AND util.[provider] ='RK900'
AND util.[sstat_desc] IN ('Session Scheduled')  -- on hold too?
AND spec.[pfmgt_spec] NOT IN ('ZZ','ZN','99')

GROUP BY DATEADD(DAY, 7 - (@@DATEFIRST-1) - DATEPART(WEEKDAY, infodb.dbo.fn_remove_time(util.[session_start_dttm])), 
		 infodb.dbo.fn_remove_time(util.[session_start_dttm])),
		 [pfmgt_spec_desc], spec.[pfmgt_spec], util.[clinic_code], util.[new_fup_status]
ORDER BY [Week End]
"""
cancer_slots = pd.read_sql_query(cancer_slots_sql, sdmart_engine)

#Initial fixing of formatting
cancer_wl['Priority'] = cancer_wl['Priority'].str.strip()
cancer_wl['Past/Future'] = 'Past'

#List of forecast weeks
fut_weeks = cancer_slots['Week End'].drop_duplicates().sort_values().astype(str).values.tolist()

################################################################################
                   #####Get start point of each scenario#####
################################################################################
#List of all the end wait list size and l6w additions to start the forecasts on
#for every possible filtering in the data.

def aggregation(cols):
    #Function to sum the wl and additions for each grouping for each week then
    #get the most recent wl size and average L6W additions ready for forecasting
    agg_df = (cancer_wl.groupby(cols + ['Week End'], as_index=False)
                                [['Waitlist Size', 'Waitlist Additions']].sum()
                       .groupby(cols, as_index=False)
                                .agg({'Waitlist Size':'last',
                                      'Waitlist Additions':'mean'}))
    return agg_df

start = pd.concat([
        #All data
        (pd.DataFrame(cancer_wl.groupby('Week End')
                      [['Waitlist Size', 'Waitlist Additions']].sum()
                      .agg({'Waitlist Size': lambda x: x.iloc[-1],
                            'Waitlist Additions': 'mean'})).T),
        #One column filtering
        aggregation(['Specialty']),
        aggregation(['Clinic Code']),
        aggregation(['Priority']),
        aggregation(['New/Follow Up']),
        #Two column filtering
        aggregation(['Specialty',   'Clinic Code']),
        aggregation(['Specialty',   'Priority']),
        aggregation(['Specialty',   'New/Follow Up']),
        aggregation(['Clinic Code', 'Priority']),
        aggregation(['Clinic Code', 'New/Follow Up']),
        aggregation(['Priority',    'New/Follow Up']),
        #Three column filtering
        aggregation(['Specialty',   'Clinic Code', 'Priority']),
        aggregation(['Specialty',   'Clinic Code', 'New/Follow Up']),
        aggregation(['Specialty',   'Priority',    'New/Follow Up']),
        aggregation(['Clinic Code', 'Priority',    'New/Follow Up']),
        #Four column filtering
        aggregation(['Specialty', 'Clinic Code', 'Priority', 'New/Follow Up'])
        ])

#Fill Nans with 0 if wl size or additions, or All if a cateorgy
start[['Waitlist Size',
       'Waitlist Additions']] = start[['Waitlist Size',
                                       'Waitlist Additions']].fillna(0)
start[['Specialty', 'Clinic Code',
       'Priority','New/Follow Up']] = start[['Specialty', 'Clinic Code',
                                             'Priority', 'New/Follow Up']
                                             ].fillna('All')


################################################################################
                #####Calculate Each Past Data and Forecast#####
################################################################################
t0 = time.time()
past_data = []
output_table = []
for cc in start['Clinic Code'].drop_duplicates().values.tolist():
    #If data is not all, filter to that clinic code
    if cc != 'All':
        cc_filter_hist = cancer_wl.loc[cancer_wl['Clinic Code'] == cc].copy()
        cc_filter_slots = cancer_slots.loc[cancer_slots['Clinic Code'] == cc].copy()
    else:
        cc_filter_hist = cancer_wl.copy()
        cc_filter_slots = cancer_slots.copy()
    #Filter the start table to those combinations
    start_filter = start.loc[start['Clinic Code'] == cc].drop('Clinic Code', axis=1)

    #Evaluate each situation for that clinic code
    for situation in start_filter.values.tolist():
        #Update variables
        WL_start, adds, spec, prior, N_FU = situation
        #print(situation)

        #If the row is for a specialty and/or appointment type filtering, add these
        #to a list of conditions for past and slots datasets.
        hist_conds = []
        slots_conds = []
        if spec != 'All':
            hist_conds.append(cancer_wl['Specialty'] == spec)
            slots_conds.append(cancer_slots['Specialty'] == spec)
    # if cc != 'All':
        #    hist_conds.append(cancer_wl['Clinic Code'] == cc)
        #   slots_conds.append(cancer_slots['Clinic Code'] == cc)
        if prior != 'All':
            hist_conds.append(cancer_wl['Priority'] == prior)
        if N_FU != 'All':
            hist_conds.append(cancer_wl['New/Follow Up'] == N_FU)
            slots_conds.append(cancer_slots['New/Follow Up'].isin(['Undefined', N_FU]))
        
        
        ######################################################Hist Data
        #Filter the historical dataset based on the conditions
        #filter_hist = cancer_wl.copy()
        filter_hist = cc_filter_hist.copy()
        for cond in hist_conds:
            filter_hist = filter_hist.loc[cond].copy()
        
        #If data is not at a granular level, we need to aggregate it up to get the
        #data for this grouping
        if len(filter_hist) > 6:
            agg_past = (filter_hist.groupby('Week End', as_index=False)
                        [['Waitlist Size', 'Waitlist Additions', 'Attendances']]
                        .sum().values.tolist())
        else:
            agg_past = filter_hist[['Week End', 'Waitlist Size',
                    'Waitlist Additions', 'Attendances']].values.tolist()

        #For each of the past 6 weeks, append the data to the past data list
        for row in agg_past:
            week, wl_size, add, att = row
            past_data.append([week, spec, cc, prior, N_FU, wl_size, add, att, 'Past', np.nan])

        ######################################################Forecast
        #Filter the slots dataset based on the conditions
        #filter_slots = cancer_slots.copy()
        filter_slots = cc_filter_slots.copy()
        for cond in slots_conds:
            filter_slots = filter_slots.loc[cond].copy()

        #If multiple rows for the Weeks, then group up by week
        if len(filter_slots) > 3:
            all_filter_slots = filter_slots.groupby('Week End')['Slots'].sum()

            #Do a version with undefined removed if it exists to produce 2 forecasts.
            if 'Undefined' in filter_slots['New/Follow Up'].values:
                filter_slots = (filter_slots.loc[
                                filter_slots['New/Follow Up']!= 'Undefined']
                                .groupby('Week End')['Slots'].sum())
            #If no undefined, then both forecasts will be the same.
            else:
                filter_slots = all_filter_slots
        else:
            #If 3 or less rows, then all filter slots is the same as filter slots.
            all_filter_slots = filter_slots
        
        #Calculate future wait list position
        fut_WL_inc_undef = []
        fut_WL_exc_undef = []
        WL_inc_undef = WL_start
        WL_exc_undef = WL_start
        for week in fut_weeks:
            #Calculate the future waitlist including undefined
            try: #If week isnt in the data, make 0
                slots_inc_undef = all_filter_slots.loc[week]
            except:
                slots_inc_undef = 0
            new_WL_inc_undef = WL_inc_undef + adds - slots_inc_undef
            fut_WL_inc_undef.append(new_WL_inc_undef)
            WL_inc_undef = new_WL_inc_undef

            output_table.append(
                        [week, spec, cc, prior, N_FU, round(new_WL_inc_undef),
                        round(adds), slots_inc_undef, 'Forecast', 'Y'])

            #Calculate the future waitlist excluding undefined
            try: #If week isnt in the data, make 0
                slots_exc_undef = filter_slots.loc[week]
            except:
                slots_exc_undef = 0
            new_WL_exc_undef = WL_exc_undef + adds - slots_exc_undef
            fut_WL_exc_undef.append(new_WL_exc_undef)
            WL_exc_undef = new_WL_exc_undef
            output_table.append(
                        [week, spec, cc, prior, N_FU, round(new_WL_exc_undef),
                        round(adds), slots_exc_undef, 'Forecast', 'N'])
        
columns = ['Week End', 'Specialty', 'Clinic Code', 'Priority', 'New/Follow Up',
           'Waitlist Size', 'Waitlist Additions', 'Attendances', 'Past/Future',
           'Including Undefined']
#Put together historical summarised data, create a copy with undefined column
historical = pd.DataFrame(past_data, columns = columns)

#historical_exc_undef = historical.copy()
#historical_exc_undef['Including Undefined'] = 'N'
#Create table of forecasts
forecast = pd.DataFrame(output_table, columns = columns)
#concat all 3 tables into one output
#wl_full_dataset = pd.concat([historical, historical_exc_undef, forecast])
wl_full_dataset = pd.concat([historical, forecast])
wl_full_dataset['Week End'] = pd.to_datetime(wl_full_dataset['Week End']).astype(str)
wl_full_dataset['Lookup Col'] = (wl_full_dataset['Week End']
                                 + wl_full_dataset['Specialty']
                                 + wl_full_dataset['Clinic Code']
                                 + wl_full_dataset['Priority']
                                 + wl_full_dataset['New/Follow Up']
                                 + wl_full_dataset['Including Undefined'].fillna(''))
wl_full_dataset =  wl_full_dataset[
                    ['Week End', 'Specialty', 'Clinic Code', 'Priority', 'New/Follow Up',
                     'Including Undefined', 'Lookup Col', 'Waitlist Size',
                     'Waitlist Additions', 'Attendances', 'Past/Future']].copy()

#Create a table template with the dates and nans to be filled in by excel formulae
wl_tabletemplate = wl_full_dataset[['Week End', 'Past/Future', 'Waitlist Size',
                                    'Waitlist Additions', 'Attendances']
                   ].drop_duplicates(subset='Week End').sort_values(by='Week End')
cols = [i for i in wl_tabletemplate.columns if (i != 'Week End' and i != 'Past/Future')]
wl_tabletemplate[cols] = np.nan

t1=time.time()
print(f'Done in {t1-t0}')


################################################################################
######Initial Set Up
writer = pd.ExcelWriter('Caner WL Forecast TEST.xlsx', engine='xlsxwriter')
workbook = writer.book

dash_ws = workbook.add_worksheet('Dash')
writer.sheets['Dash'] = dash_ws

fulldata_ws = workbook.add_worksheet('Full Data')
writer.sheets['Full Data'] = fulldata_ws

lookup_ws = workbook.add_worksheet('Look Up')
writer.sheets['Look Up'] = lookup_ws

######Formats
#White background
bg_format = workbook.add_format({'font_size':12, 'align':'centre',
                                 'valign':'centre', 'bg_color':'white',
                                 'text_wrap':True})
#Filter box formats
header_format = workbook.add_format({'font_size':18, 'bold':True, 'align':'centre', 'valign':'centre',  'border':True, 'text_wrap':True})
filter_format1 = workbook.add_format({'font_size':14, 'bold':True, 'align':'centre', 'valign':'centre', 'border':True,})
filter_format2 = workbook.add_format({'font_size':14, 'bold':True, 'align':'centre', 'valign':'centre', 'border':True, 'bg_color':'yellow'})

######Dashboard
wl_tabletemplate.to_excel(writer, sheet_name='Dash', index=False,
                          startrow=10, startcol=1)

#White background and default column widths
dash_ws.set_column('A:AE', 15, bg_format)
dash_ws.set_column('A:A', 4, bg_format)
dash_ws.set_column('G:G', 4, bg_format)
dash_ws.set_row(2, None,  bg_format)
for row in range(0, 9):
    dash_ws.set_row(row + 2, 21)

    ##Filter section
#definition column
#dash_ws.set_column('B:C', 30, bg_format)
dash_ws.merge_range('B2:E2', 'Filters', header_format)
dash_ws.merge_range('B3:C3', 'Specialty', filter_format1)
dash_ws.merge_range('B4:C4', 'Clinic Code', filter_format1)
dash_ws.merge_range('B5:C5', 'Priority', filter_format1)
dash_ws.merge_range('B6:C6', 'New/Follow Up', filter_format1)
dash_ws.merge_range('B7:C7', 'Including Undefined', filter_format1)
dash_ws.merge_range('B8:E8', '=D3&D4&D5&D6', filter_format1)
#selection column
no_spec = wl_full_dataset['Specialty'].nunique() + 1
no_cc = wl_full_dataset['Clinic Code'].nunique() + 1
dash_ws.data_validation('D3', {'validate':'list', 'source':f"'Look Up'!A2:A{no_spec}"})
dash_ws.merge_range('D3:E3', 'All', filter_format2)
dash_ws.data_validation('D4', {'validate':'list', 'source':f"'Look Up'!C2:C{no_cc}"})
dash_ws.merge_range('D4:E4', 'All', filter_format2)
dash_ws.data_validation('D5', {'validate':'list', 'source':wl_full_dataset['Priority'].drop_duplicates().tolist()})
dash_ws.merge_range('D5:E5', 'All', filter_format2)
dash_ws.data_validation('D6', {'validate':'list', 'source':wl_full_dataset['New/Follow Up'].drop_duplicates().tolist()})
dash_ws.merge_range('D6:E6', 'All', filter_format2)
dash_ws.data_validation('D7', {'validate':'list', 'source':wl_full_dataset['Including Undefined'].drop_duplicates().tolist()})
dash_ws.merge_range('D7:E7', 'Y', filter_format2)

    ##Table section
#Populate the table with vlookups
for row in range(12, 18):
    dash_ws.write(f'D{row}', f"=VLOOKUP(B{row}&B8,'Full Data'!G:J,2,0)")
    dash_ws.write(f'E{row}', f"=VLOOKUP(B{row}&B8,'Full Data'!G:J,3,0)")
    dash_ws.write(f'F{row}', f"=VLOOKUP(B{row}&B8,'Full Data'!G:J,4,0)")
for row in range(18, 21):
    dash_ws.write(f'D{row}', f"=VLOOKUP(B{row}&B8&D7,'Full Data'!G:J,2,0)")
    dash_ws.write(f'E{row}', f"=VLOOKUP(B{row}&B8&D7,'Full Data'!G:J,3,0)")
    dash_ws.write(f'F{row}', f"=VLOOKUP(B{row}&B8&D7,'Full Data'!G:J,4,0)")

    ##Line Graph section
WL_chart = workbook.add_chart({'type':'line'})
WL_chart.add_series({'name':'Wait List',
                     'categories':'=Dash!$B$12:$B$20',
                     'values':'=Dash!$D$12:$D$20',
                     'data_labels': {'value': True,
                                     'position': 'above'},
                     'smooth':True,
                     'marker': {'type': 'automatic'},})
WL_chart.set_title({'Name':'Wait List'})
dash_ws.insert_chart('H2', WL_chart, {'x_scale': 2.7, 'y_scale': 1.1})

    #Bar Chart section
att_add_chart = workbook.add_chart({'type':'column'})
att_add_chart.add_series({'name':'Additions',
                          'categories':'=Dash!$B$12:$B$20',
                          'values':'=Dash!$E$12:$E$20',
                          'data_labels': {'value': True}})
att_add_chart.add_series({'name':'Attendances',
                          'categories':'=Dash!$B$12:$B$20',
                          'values':'=Dash!$F$12:$F$20',
                          'data_labels': {'value': True}})
att_add_chart.set_title({'Name':'Additions and Attendances'})
dash_ws.insert_chart('H15', att_add_chart, {'x_scale': 2.7, 'y_scale': 1.1})

######Full Data Set
wl_full_dataset.to_excel(writer, sheet_name='Full Data', index=False)

######Lookup sheet
wl_full_dataset['Specialty'].drop_duplicates().to_excel(writer, sheet_name='Look Up', index=False)
wl_full_dataset['Clinic Code'].drop_duplicates().to_excel(writer, sheet_name='Look Up', index=False, startcol=2)

writer.close()

X=5

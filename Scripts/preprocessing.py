import pandas as pd
import csv
import argparse
import yaml
import os

header = True

def readCsv():
	df = pd.read_csv(open(data_path,'rU'), encoding = 'utf-8', engine = 'c')
	return df

def GroupById(df):
	df = df.groupby('SUBJECT_ID', as_index = False).agg(lambda x: x.tolist())
	return df

def DataFrameGen():
	dff = pd.DataFrame()
	data = pd.DataFrame({"SUBJECT_ID": []})
	dff = dff.append(data, sort=False)
	for i in range(122):
	     data = pd.DataFrame({"HADM_ID" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"ICUSTAY_ID" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"LAST_CAREUNIT" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"DBSOURCE" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"INTIME" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"OUTTIME" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"LOS" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"ADMITTIME" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"DISCHTIME" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"ETHNICITY" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"DIAGNOSIS" + str(i): []})
	     dff = dff.append(data, sort=False)
	     data = pd.DataFrame({"AGE" + str(i): []})
	     dff = dff.append(data, sort=False)
	global header
	if(header == True):
		saveCsv(dff, True)
		header = False
	return dff
 
def FillValues(index, last_index, data_range):
	df = readCsv()
	df = GroupById(df)
	dff = DataFrameGen()
	data = pd.DataFrame({"SUBJECT_ID": range(index - last_index)})
	dff = dff.append(data, sort=False)
	for i in range(index - last_index):
		k = i + last_index
		dff['SUBJECT_ID'][i] = df['SUBJECT_ID'][k]
		for j in range(len(df['HADM_ID'][k])):
			dff['HADM_ID' + str(j)][i] = df['HADM_ID'][k][j]
			dff['LAST_CAREUNIT' + str(j)][i] = df['LAST_CAREUNIT'][k][j]
			dff['DBSOURCE' + str(j)][i] = df['DBSOURCE'][k][j]
			dff['ICUSTAY_ID' + str(j)][i] = df['ICUSTAY_ID'][k][j]
			dff['INTIME' + str(j)][i] = df['INTIME'][k][j]
			dff['OUTTIME' + str(j)][i] = df['OUTTIME'][k][j]
			dff['ADMITTIME' + str(j)][i] = df['ADMITTIME'][k][j]
			dff['DISCHTIME' + str(j)][i] = df['DISCHTIME'][k][j]
			dff['ETHNICITY' + str(j)][i] = df['ETHNICITY'][k][j]
			dff['DIAGNOSIS' + str(j)][i] = df['DIAGNOSIS'][k][j]
			dff['AGE' + str(j)][i] = df['AGE'][k][j]
	saveCsv(dff, False)
	return


def saveCsv(dff, header):
	if(header == True):
		dff.to_csv(output_path, mode = 'a', header = True, index = False)
	else:
		dff.to_csv(output_path, mode = 'a', header = False, index = False)
	return

if __name__ == "__main__":
	data_range = 30
	last_index = 0
	parser = argparse.ArgumentParser(description='Extract per-subject data from MIMIC-III CSV files.')
	parser.add_argument('data_path', type=str, help='Directory containing Data CSV files.')
	parser.add_argument('output_path', type=str, help='Directory where per-subject data should be written.')
	parser.add_argument('index', type=int, help='size to read at a time.')
	parser.add_argument('iteration', type=int, help='number of times size should be read.')
	args, _ = parser.parse_known_args()
	index = args.index
	iteration = args.iteration
	data_path = args.data_path
	output_path = args.output_path
	while iteration > 0 :
		FillValues(index, last_index, data_range)
		last_index = index
		index *= 2
		iteration -= 1;

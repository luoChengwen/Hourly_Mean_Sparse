import pandas as pd

path = '/Users/daisy/Dropbox/test_different_time_slot/Report_com/'

dataFix = pd.read_pickle(path + 'fixed_segoutputResults.pickle')
dataFlex = pd.read_pickle(path + 'flexible_segoutputResults.pickle')

a = pd.DataFrame(dataFix[['r','overlapH']].groupby(dataFix['condition']).mean())
a['cond'] = 'fix'

b = pd.DataFrame(dataFlex[['r','overlapH']].groupby(dataFlex['condition']).mean())
b['cond'] = 'flex'
pd.concat([a,b]).to_csv(path + 'final.csv')

#
# bp = dataFix['r'].groupby(dataFix['condition']).plot(kind='kde',legend=True)
# bp2 = dataFlex['r'].groupby(dataFix['condition']).plot(kind='kde',legend=True,title='Flex')

# import pyvttbl as pt
# aov = dataFlex.anova('r', sub='ID', wfactors=['condition'])
# print(aov)

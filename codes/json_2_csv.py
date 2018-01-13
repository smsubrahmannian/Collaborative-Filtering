import json
import pandas as pd

def convert(x):
    ''' Convert a json string to a flat python dictionary
    which can be passed into Pandas. '''
    ob = json.loads(x)
    for k, v in ob.items():
        if isinstance(v, list):
            ob[k] = ','.join(v)
        elif isinstance(v, dict):
            for kk, vv in v.items():
                ob['%s_%s' % (k, kk)] = vv
            del ob[k]
    return ob
json_filename='review.json'
with open(json_filename,'rb') as f:
    data = f.readlines()

df = pd.DataFrame([convert(line) for line in data])

df_final = df.loc[:,['user_id','business_id','stars']]
df_final.head()

business_2_ix = {o:i for i,o in enumerate(set(df_final.business_id))}
userid_2_ix = {o:i for i,o in enumerate(set(df_final.user_id))}

df_final['business_id'] = df_final.business_id.map(business_2_ix).values
df_final['user_id'] = df_final.user_id.map(userid_2_ix).values

df_final.to_csv('./reviews.csv',index=False)
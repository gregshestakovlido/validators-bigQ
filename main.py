import streamlit as st
import scripts
import plotly.express as px
import pandas as pd

st.title('Validators performance exctractor')

if 'val_NO_choice' not in st.session_state:
    val_NO_list= []
    st.session_state['val_NO_choice'] = val_NO_list

if 'val_choice' not in st.session_state:
    val_list= []
    st.session_state['val_choice'] = val_list

if 'one_NO_choice' not in st.session_state:
    val_list= []
    st.session_state['one_NO_choice'] = val_list

operator_list=['Allnodes',
 'Anyblock Analytics',
 'Blockdaemon',
 'Blockscape',
 'BridgeTower',
 'Certus One',
 'ChainLayer',
 'ChainSafe',
 'Chorus One',
 'ConsenSys Codefi',
 'CryptoManufaktur',
 'DSRV',
 'Everstake',
 'Figment',
 'HashQuark',
 'InfStones',
 'Kukis Global',
 'Nethermind',
 'P2P.ORG - P2P Validator',
 'Prysmatic Labs',
 'RockLogic GmbH',
 'RockX',
 'Sigma Prime',
 'Simply Staking',
 'SkillZ',
 'Stakely',
 'Stakin',
 'Staking Facilities',
 'stakefish']


st.write('**One NO info (with visualization)**')
one_NO_select=st.selectbox(
        "Please select operator",
    operator_list,key='one_NO_select')

start_date=st.date_input("Choose start date",key='one_NO_start_date')

end_date = st.date_input("Choose end date",key='one_NO_end_date')

st.write('Please remember that each request cost approximately 6 dollars and processing time is about 2.5 minutes per 1 day per 1 NO')
if st.button('Get info'):


 one_no_choice_dict={}
 one_no_choice_dict['start_date']=str(start_date)
 one_no_choice_dict['end_date']=str(end_date)
 one_no_choice_dict['node_operator'] = one_NO_select
 st.session_state['one_NO_choice'].append(one_no_choice_dict)


 st.write('Start fetching data')

 rewards_df=scripts.create_rewards_table(st.session_state['one_NO_choice'])
 st.write('All data are fetched, prepare visualization')

 processed_reward_df=scripts.process_rewards_table(rewards_df)
 st.write(f'Results for {one_NO_select} from {start_date} to {end_date}')
 total_result_chart = px.bar(processed_reward_df, x='epoch_date', y=['total_rewards', 'total_penalties', 'total_missed_rewards'],
                             title='Totals')
 rewards_chart = px.bar(processed_reward_df, x='epoch_date',
                        y=['total_rewards_for_proposal', 'total_rewards_for_source', 'total_rewards_for_target',
                           'total_rewards_for_head', 'total_rewards_for_sync'], title='Detailed rewards')
 penalties_chart = px.bar(processed_reward_df, x='epoch_date', y=['total_penalty_for_source', 'total_penalty_for_target',
                                                         'total_penalty_for_sync'], title='Detailed penalties')
 missed_rewards_chart = px.bar(processed_reward_df, x='epoch_date',
                               y=['total_missed_reward_for_head', 'total_missed_reward_for_proposal'],
                               title='Detailed missed rewards')
 quantity_chart = px.bar(processed_reward_df, x='epoch_date',
                         y=['count_sync_cometee_participation','count_proposals', 'count_missed_proposals', 'count_missed_for_source',
                            'count_missed_for_target', ], title='Quantity data')
 st.plotly_chart(total_result_chart, use_container_width=True)
 st.plotly_chart(rewards_chart, use_container_width=True)
 st.plotly_chart(penalties_chart, use_container_width=True)
 st.plotly_chart(missed_rewards_chart, use_container_width=True)
 st.plotly_chart(quantity_chart, use_container_width=True)

 csv_data_validators=processed_reward_df.to_csv(index=False).encode('utf-8')

 st.write('WARNING - once you download data, all graphs will disappear')
 st.download_button(label='Download info',
                    data=csv_data_validators,
                    file_name=f'{one_NO_select} from {start_date} to {end_date}.csv',
                    mime='text/csv',
                    key='one_NO_download')

st.markdown("""---""")
st.write('**Several NO info (just data)**')

st.write('Choose NO and date range and press "Add" button, once you choose all NO and date range for them press "Get info" button')


several_NO_select = st.selectbox(
        "Please select operator",
        operator_list, key='multiple_NO_select')
several_NO_start_date = st.date_input("Choose start date",key='multiple_NO_start_date')

several_NO_end_date = st.date_input("Choose end date",key='multiple_NO_end_date')
add_NO=st.button('Add',key='multiple_NO_add')


if add_NO:
    choice={}
    choice['node_operator']=several_NO_select
    choice['start_date']= str(several_NO_start_date )
    choice['end_date']=str(several_NO_end_date)
    st.session_state['val_NO_choice'].append(choice)

st.write('You already choose:')
st.write(st.session_state['val_NO_choice'])

get_multiple_NO_info=st.button('Get info',key='multiple_NO_download')
if get_multiple_NO_info:
    st.write('Start fetching data')
    NOs_rewards_df=scripts.create_rewards_table(st.session_state['val_NO_choice'])
    processed_NOs_rewards_df=scripts.process_rewards_table(NOs_rewards_df)
    st.write(processed_NOs_rewards_df.head(10))
    csv_NOs_data_validators = processed_NOs_rewards_df.to_csv(index=False).encode('utf-8')
    st.download_button(label='Download info',
                           data=csv_NOs_data_validators,
                           file_name=f'Multiple Nos data .csv',
                           mime='text/csv',
                           key='multiple_NO_download' )





st.markdown("""---""")
st.write('This is test section')
if st.button('Test'):
    validators=scripts.get_validators_by_epoch()
    st.table(validators.head(10))
import streamlit as st
import scripts
import datetime
import plotly.express as px
import pandas as pd

st.title('Validators performance exctractor')

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

operator_select=st.selectbox(
        "Please select operator",
    operator_list)

start_date=st.date_input("Choose start date")

end_date = st.date_input("Choose end date")

st.write('Please remember that each request cost approximately 6 dollars and processing time is about 2.5 minutes per 1 day per 1 NO')
if st.button('Get info'):

 start_epoch=scripts.convert_date_to_epoch(str(start_date))+1
 end_epoch = scripts.convert_date_to_epoch(str(end_date))+225

 st.write('Start fetching data')
 rewards_df=scripts.create_rewards_table(start_epoch=start_epoch,end_epoch=end_epoch,operator_name=operator_select)
 st.write('All data are fetched, prepare visualization')

 processed_reward_df=scripts.process_rewards_table(rewards_df)
 st.write(f'Results for {operator_select} from {start_date} to {end_date}')
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
                    file_name=f'{operator_select} from {start_date} to {end_date}.csv',
                    mime='text/csv')



if st.button('Test'):
    validators=scripts.get_validators_by_epoch()
    st.table(validators.head(10))








import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import time
import datetime
import json


TOKEN=st.secrets['dune_key']


JSON_TOKEN=json.loads(TOKEN,strict=False)

CREDENTIALS = service_account.Credentials.from_service_account_info(JSON_TOKEN)

CLIENT = bigquery.Client(credentials=CREDENTIALS, project=CREDENTIALS.project_id)

REWARDS_QUERY="""
SELECT
DATE_TRUNC(TIMESTAMP_SECONDS(1606824023 + (f_epoch * 12 * 32)),DAY) as epoch_date,
count(nullif(sync_cometee_participation,0)) as count_sync_cometee_participation,
sum(reward_for_proposal) as total_rewards_for_proposal,
sum(reward_for_source) as total_rewards_for_source,
sum(reward_for_target) as total_rewards_for_target,
sum(reward_for_head) as total_rewards_for_head,
sum(reward_for_sync) as total_rewards_for_sync,
sum(penalty_for_source) as total_penalty_for_source,
sum(penalty_for_target) as total_penalty_for_target,
sum(penalty_for_sync) as total_penalty_for_sync,
sum(missed_reward_for_head) as total_missed_reward_for_head,
sum(missed_reward_for_proposal) as total_missed_reward_for_proposal,
count(nullif(reward_for_proposal,0)) as count_proposals,
count(nullif(missed_reward_for_proposal,0)) as count_missed_proposals,
count(nullif(penalty_for_source,0)) as count_missed_for_source,
count(nullif(penalty_for_target,0)) as count_missed_for_target
FROM `high-hue-328212.chaind.mv_validators_rewards_per_epoch`
where
f_epoch between @start_epoch and @end_epoch
and
f_validator_index in (SELECT f_index FROM `high-hue-328212.chaind.t_operators` where f_operator_name=@operator_name)
group by  epoch_date
"""



VALIDATORS_QUERY="""
with active_vals as (
SELECT
count(f_index) as active_validators,
f_activation_epoch  as activation_epoch
FROM `high-hue-328212.chaind.t_validators`
where f_index in (SELECT f_index FROM `high-hue-328212.chaind.t_operators`)
group by activation_epoch),
all_vals as (
SELECT
count(f_index) as all_validators,
f_activation_eligibility_epoch as deposit_epoch
FROM `high-hue-328212.chaind.t_validators`
where f_index in (SELECT f_index FROM `high-hue-328212.chaind.t_operators`)
group by deposit_epoch)


select
f_epoch,active_vals.active_validators ,all_vals.all_validators
from
high-hue-328212.chaind.t_epoch_summaries
left join active_vals on active_vals.activation_epoch=f_epoch
left join all_vals on all_vals.deposit_epoch=f_epoch
order by f_epoch asc
"""
def convert_date_to_epoch(date):
    epoch_ts=time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())
    epoch= (epoch_ts-1606824023+10800)/(12*32)
    return int(epoch)

def create_rewards_table(start_epoch,end_epoch,operator_name):

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_epoch", "INT64", start_epoch),
            bigquery.ScalarQueryParameter("end_epoch", "INT64", end_epoch),
            bigquery.ScalarQueryParameter("operator_name", "STRING", operator_name)
        ]
    )

    query_job = CLIENT.query(REWARDS_QUERY, job_config=job_config)


    rewards_df=query_job.to_dataframe()
    return rewards_df


def process_rewards_table(rewards_df):
    rewards_df[['total_rewards_for_proposal', 'total_rewards_for_source', 'total_rewards_for_target',
                'total_rewards_for_head', 'total_rewards_for_sync', 'total_penalty_for_source',
                'total_penalty_for_target',
                'total_penalty_for_sync', 'total_missed_reward_for_head', 'total_missed_reward_for_proposal']] \
        = rewards_df[['total_rewards_for_proposal', 'total_rewards_for_source', 'total_rewards_for_target',
                      'total_rewards_for_head', 'total_rewards_for_sync', 'total_penalty_for_source',
                      'total_penalty_for_target',
                      'total_penalty_for_sync', 'total_missed_reward_for_head',
                      'total_missed_reward_for_proposal']].div(10 ** 9, axis=0)

    rewards_df['total_rewards'] = rewards_df[
        ['total_rewards_for_proposal', 'total_rewards_for_source', 'total_rewards_for_target',
         'total_rewards_for_head', 'total_rewards_for_sync']].sum(axis=1)

    rewards_df['total_penalties'] = rewards_df[['total_penalty_for_source', 'total_penalty_for_target',
                                                'total_penalty_for_sync']].sum(axis=1)

    rewards_df['total_missed_rewards'] = rewards_df[
        ['total_missed_reward_for_head', 'total_missed_reward_for_proposal']].sum(axis=1)

    return rewards_df


def get_validators_by_epoch ():
    '''Generate table with epoch|active_validatros|total-validators starting from the merge'''
    query_job_validators = CLIENT.query(VALIDATORS_QUERY)
    validators_df = query_job_validators.to_dataframe()
    validators_df = validators_df.fillna(0)
    validators_df['active_validators'] = validators_df['active_validators'].cumsum()
    validators_df['total_validators'] = validators_df['all_validators'].cumsum()
    result = validators_df[['f_epoch', 'active_validators', 'total_validators']]
    result = result[result['f_epoch'] >= 146875]
    return result
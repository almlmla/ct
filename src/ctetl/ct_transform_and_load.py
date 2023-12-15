# ct_transform_and_load

import pandas as pd

def recast_accounts(accounts_df):
    """
    Recast dtypes prior to uploading to PostgreSQL.
    """

    recast_accounts_df = pd.DataFrame()
    
    recast_accounts_df = accounts_df.astype({'account_id':'int64',
                                    'account_name':'string',
                                    'account_handle':'string',
                                    'account_url':'string',
                                    'account_platform':'string',
                                    'account_platform_id':'int64',
                                    'account_type':'string',
                                    'account_page_admin_top_country':'string',
                                    'account_page_description':'string',
                                    'account_page_created_date':'datetime64[ns]',
                                    'account_page_category':'string'
                                   })
    
    recast_accounts_df['account_verified'] = accounts_df['account_verified'].map({'False':False, 'True':True}).astype('bool')

    return(recast_accounts_df)


def recast_posts(posts_df):
    """
    Recast dtypes prior to uploading to PostgreSQL.
    """

    recast_posts_df = pd.DataFrame()
    
    recast_posts_df = posts_df.astype({'platform_id':'string',
                                 'platform':'string',
                                 'posting_date':'datetime64[ns]',
                                 'post_type':'string',
                                 'post_message':'string',
                                 'post_url':'string',
                                 'subscriber_count':'int64',
                                 'account_id':'int64'
                                })

    return(recast_posts_df)


def recast_post_metrics(post_metrics_df):
    """
    Recast dtypes prior to uploading to PostgreSQL.
    """

    recast_post_metrics_df = pd.DataFrame()
    
    recast_post_metrics_df = post_metrics_df.astype({'platform_id':'string',
                                        'as_of':'datetime64[ns]',
                                        'score':'float64',
                                        'metric_name':'string',
                                        'metric_value':'int64',
                                        'metric_timestamp':'datetime64[ns]',
                                        'metric_timestep':'int64'
                                       })

    return(recast_post_metrics_df)




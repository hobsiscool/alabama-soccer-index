import pandas as pd
from datetime import datetime, timedelta

def calculate_rankings(df, iterations=100, hfa=0.35):
    # Ensure a fresh copy to avoid modifying the original dataframe
    working_df = df.copy()
    
    # 1. Create the MOV column as float to handle decimal HFA adjustments
    working_df['mov'] = (working_df['score_f'] - working_df['score_a']).clip(-3, 3).astype(float)
    
    # 2. Adjust for Home Field Advantage (HFA)
    working_df.loc[working_df['is_home'], 'mov'] -= hfa
    
    teams = pd.unique(working_df[['team', 'opponent']].values.ravel())
    ratings = pd.Series(0.0, index=teams)
    sos = pd.Series(0.0, index=teams)
    
    # 3. Iterative SRS calculation (Strength of Schedule adjustment)
    for _ in range(iterations):
        for team in teams:
            t_games = working_df[working_df['team'] == team]
            o_games = working_df[working_df['opponent'] == team]
            if (len(t_games) + len(o_games)) == 0: continue
            
            perf = t_games['mov'].sum() - o_games['mov'].sum()
            opp_list = pd.concat([t_games['opponent'], o_games['team']])
            avg_sos = ratings[opp_list].mean()
            
            ratings[team] = (perf / len(opp_list)) + avg_sos
            sos[team] = avg_sos
            
    return pd.DataFrame({'Rating': ratings, 'SOS': sos})

def get_rankings_with_trend(df, iterations=100, hfa=0.35):
    # Calculate CURRENT rankings
    current_results = calculate_rankings(df, iterations, hfa).sort_values('Rating', ascending=False)
    current_results['Current_Rank'] = range(1, len(current_results) + 1)
    
    # Calculate PAST rankings (Snapshot from 7 days ago)
    df['dt'] = pd.to_datetime(df['game_date'], errors='coerce')
    one_week_ago = datetime.now() - timedelta(days=7)
    past_df = df[df['dt'] < one_week_ago].copy()
    
    if not past_df.empty:
        past_results = calculate_rankings(past_df, iterations, hfa).sort_values('Rating', ascending=False)
        past_results['Past_Rank'] = range(1, len(past_results) + 1)
        
        # Merge to find the difference
        merged = current_results.join(past_results[['Past_Rank']], how='left')
        # Trend = Past Rank - Current Rank (Positive number means rank improved/went up)
        merged['Trend'] = merged['Past_Rank'] - merged['Current_Rank']
        return merged.fillna(0)
    
    current_results['Trend'] = 0
    return current_results
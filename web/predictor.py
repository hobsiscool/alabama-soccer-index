import numpy as np
from scipy.stats import poisson

def predict_matchup(home_r, away_r, avg_g=1.4, hfa=0.35):
    # Neutral site doesn't add HFA
    h_lambda = max(0.1, avg_g + (home_r - away_r) + hfa)
    a_lambda = max(0.1, avg_g + (away_r - home_r))
    
    # Calculate probabilities
    max_g = 7
    probs = [[poisson.pmf(h, h_lambda) * poisson.pmf(a, a_lambda) for a in range(max_g)] for h in range(max_g)]
    probs = np.array(probs)
    
    return {
        'h_win': np.sum(np.tril(probs, -1)),
        'a_win': np.sum(np.triu(probs, 1)),
        'draw': np.sum(np.diag(probs)),
        'score': np.unravel_index(probs.argmax(), probs.shape)
    }
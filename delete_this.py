import numpy as np
from sklearn.metrics import confusion_matrix

# Inputs:
# y_true: true binary recidivism outcomes
# y_scores: continuous prediction scores from the COMPAS model
# protected_attribute: array indicating protected (1) or privileged (0) group membership

# Outputs:
# thresholds: dictionary of thresholds per group ensuring equalized odds
# y_pred_adjusted: binary predictions adjusted to enforce equalized odds

def equalized_odds_postprocessing(y_true, y_scores, protected_attribute):
    thresholds = {}
    groups = [0, 1]  # privileged and unprivileged

    # Grid search threshold to match error rates
    def get_error_rates(y_true, y_pred):
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
        fpr = fp / (fp + tn)  # false positive rate
        fnr = fn / (fn + tp)  # false negative rate
        return fpr, fnr

    target_fpr, target_fnr = [], []

    # Determine optimal thresholds per group
    for group in groups:
        scores_group = y_scores[protected_attribute == group]
        true_group = y_true[protected_attribute == group]
        best_diff, best_thresh = float('inf'), 0.5

        # Search thresholds in increments of 0.01
        for thresh in np.arange(0, 1.01, 0.01):
            preds = (scores_group >= thresh).astype(int)
            fpr, fnr = get_error_rates(true_group, preds)
            diff = abs(fpr - fnr)
            if diff < best_diff:
                best_diff = diff
                best_thresh = thresh

        thresholds[group] = best_thresh
        target_fpr.append(fpr)
        target_fnr.append(fnr)

    # Match error rates between groups by adjusting thresholds further
    avg_fpr = np.mean(target_fpr)
    avg_fnr = np.mean(target_fnr)

    for group in groups:
        scores_group = y_scores[protected_attribute == group]
        true_group = y_true[protected_attribute == group]
        best_diff, best_thresh = float('inf'), thresholds[group]

        # Find closest threshold matching average rates
        for thresh in np.arange(0, 1.01, 0.01):
            preds = (scores_group >= thresh).astype(int)
            fpr, fnr = get_error_rates(true_group, preds)
            diff = abs(fpr - avg_fpr) + abs(fnr - avg_fnr)
            if diff < best_diff:
                best_diff = diff
                best_thresh = thresh

        thresholds[group] = best_thresh

    # Apply adjusted thresholds
    y_pred_adjusted = np.array([
        1 if y_scores[i] >= thresholds[protected_attribute[i]] else 0
        for i in range(len(y_scores))
    ])

    return thresholds, y_pred_adjusted

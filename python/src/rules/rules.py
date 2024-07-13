# 1. is_score_rejected() is to check the credit score and see whether it is less than threshold value for rejection.
def is_score_rejected(score):
    '''
        This function is to get the credit score of the particular card which transaction happened and see whether it has greater than or equal to 200. If not then it is labelled as Fraud.
        Input Arguments: score (credit_score from card_transactions real time data - Kafka topic in JSON format)
    '''
    if score < 200:
        return True
    return False
    

# 2. is_ucl_exceeded() is to check whether the amount exceeds the upper control limit of the transaction to be done.
def is_ucl_exceeded(amount, ucl):
    '''
        This function is to check whether the amount exceeds the upper control limit of the transaction to be done. If it is exceeded then it is labelled as Fraud.
        Input Arguments: amount (card_transactions real time data - Kafka topic in JSON format)
                        ucl (UCL attribute value from card_lookup table - HBase)
    '''
    if amount > ucl:
        return True
    return False
    
# 3. is_zipcode_invalid() is to check whether the travelling speed between current and last transaction happened is valid or not.  
def is_zipcode_invalid(distance, time_diff_secs):
    '''
        This function is to measure the speed in Kilometers/second travelled by the customer between two locations where customer made the recent two transactions (current and previous).
        Usually commercial airplanes travel with an average speed of around 800-950 Kilometers/hour. We considered the speed threshold value as 900 Kilometers/hour and converted it to seconds which is (900/3600) = 0.25 Kilometers/second. If the measure speed between two locations is greater than 0.25 Kilometers/second then it is labelled as Fraud.
        Input Arguments: distance (distance convered between two locations of recent two transactions)
                        time_diff_secs (Time difference between last two transactions happened)
    '''
    if time_diff_secs == 0:
        if distance != 0:
            return True
    else:
        speed_kmps = distance / time_diff_secs
        if speed_kmps > 0.25:
            return True
    return False
    
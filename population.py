import pandas as pd
import requests
from sklearn import linear_model
from sklearn.metrics import mean_squared_error

def get_json(url):
    """
    :param url: RUL of the resource
    :return: json
    """
    resp = requests.get(url=url)
    data = resp.json()
    return data

def load_pop(json_obj, split_percentage):
    json_data = json_obj['result']['population_history']
    columns = ["year", "current_pop"]
    df = pd.DataFrame(data=json_data, columns=columns)

    pop_x = df.drop('current_pop', axis=1).values
    pop_y = df['current_pop'].values

    # Split the dataset in train and test data
    # A random permutation, to split the data randomly

    split_point = int(len(pop_x) * split_percentage)
    pop_X_train = pop_x[:split_point]
    pop_y_train = pop_y[:split_point]
    pop_X_test = pop_x[split_point:]
    pop_y_test = pop_y[split_point:]
    return pop_X_train, pop_y_train, pop_X_test, pop_y_test



if __name__ == "__main__":
    """
    url = "http://localhost:5000/population/history/NSW"
    pred_year = 2019
    json_obj = get_json(url)
    """
    pred_year = 2019
    state_id = 'NSW'

    url = 'http://127.0.0.1:5000/population/history'
    req = requests.get(url)
    req_table = req.json()
    table = next((item for item in req_table['result'] if item['state_id'] == state_id))
    data = table['population_history']
    print(data)

    #json_data = json_obj['result']['population_history']
    columns = ["year", "current_pop"]
    df = pd.DataFrame(data=data, columns=columns)

    pop_X_train = df.drop('current_pop', axis=1).values
    pop_y_train = df['current_pop'].values

    model = linear_model.LinearRegression()
    model.fit(pop_X_train, pop_y_train)

    ftr = pd.DataFrame({'Year': [pred_year]})
    print(ftr)
    future = model.predict(ftr)
    """
    for i in range(len(ftr)):
        print(i)
        print("Predicted:", future[i])
    """
    print(future)

    # The mean squared error
    #print("Mean squared error: %.2f"
    #      % mean_squared_error(pop_y_test, y_pred))

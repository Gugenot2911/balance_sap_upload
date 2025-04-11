from flask import Flask, jsonify, request
from flask.json.provider import DefaultJSONProvider
import json
from flask_cors import CORS
import os
import balance
import df_report
import polars as pl


pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(9999)
pl.Config.set_fmt_str_lengths(100)

class CustomJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        # Убедимся, что кириллица не преобразуется в Unicode-последовательности
        return json.dumps(obj, ensure_ascii=False, **kwargs)

app = Flask(__name__)
app.json_encoder = CustomJSONProvider
CORS(app)


# os.chdir(r'C:/Users/ruslan.lavrov/PycharmProjects/CA_analise/')
# print(os.getcwd())

@app.route('/balance/<string:site_name>', methods=['GET'])

def balance_os(site_name):

    balance_site = balance.Balance(site_name=site_name)
    refund_logistic = balance.Balance(site_name).refund()

    response = {'balance_site': balance_site.sap_os().to_dicts(),
                'refund_logistic': refund_logistic.to_dicts()}

    return jsonify(response)

@app.route('/storage/<string:site_name>', methods=['GET'])

def balance_storage(site_name):

    balance_site = balance.Balance(site_name=site_name)
    return jsonify(balance_site.sap_tmc().to_dicts())


@app.route('/report', methods=['POST'])
def report():
    data = request.get_json()
    report = df_report.add_items
    if not data:
        return jsonify({"error": "No JSON data received"}), 400

    try:
        print (data)
        print (report(data=data))
        return jsonify({
            "status": "success",
            "data": data
        }), 200

    except Exception as e:
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True, ssl_context=('cert.pem', 'key.pem'))

from flask import Flask, jsonify, request
from flask.json.provider import DefaultJSONProvider
import json
from flask_cors import CORS
import os
import balance
from df_report import add_items
import df_report
import polars as pl
from functools import wraps

# Конфигурация Polars
pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_width_chars(9999)
pl.Config.set_fmt_str_lengths(100)


class CustomJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        return json.dumps(obj, ensure_ascii=False, **kwargs)


app = Flask(__name__)
app.json = CustomJSONProvider(app)
CORS(app)


def handle_errors(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            app.logger.error(f"Error in {f.__name__}: {str(e)}")
            return jsonify({"error": str(e), "status": "error"}), 500

    return wrapper


@app.route('/balance/<string:site_name>', methods=['GET'])
@handle_errors
def balance_os(site_name):
    balance_site = balance.Balance(site_name=site_name)
    balance_data = balance_site.sap_os()
    refund_data = balance_site.refund()

    if balance_data is None or refund_data is None:
        return jsonify({"error": "Failed to get balance data"}), 404

    return jsonify({
        "balance_site": balance_data.to_dicts(),
        "refund_logistic": refund_data.to_dicts()
    })


@app.route('/storage/<string:site_name>', methods=['GET'])
@handle_errors
def balance_storage(site_name):
    balance_site = balance.Balance(site_name=site_name)
    storage_data = balance_site.sap_tmc()

    if storage_data is None:
        return jsonify({"error": "Storage data not found"}), 404

    return jsonify(storage_data.to_dicts())


@app.route('/report', methods=['POST'])
@handle_errors
def report():
    data = request.get_json()

    if not data:
        return jsonify({"error": "No JSON data received"}), 400

    if not isinstance(data, (dict, list)):
        return jsonify({"error": "Invalid data format"}), 400

    result = add_items(data=data)
    return jsonify({
        "status": "success",
        "result": result
    })

# @app.route('/name_report/', methods=['GET'])
# def name_report():
#     reports_list = df_report.files
    return reports_list

@app.route('/name_report/', methods=['GET'])
def upload_reports():
    file_name = df_report.files
    report = df_report.read_reports(file_name, as_dataframes=False)

    return report


if __name__ == '__main__':
    ssl_context = None
    if os.path.exists('cert.pem') and os.path.exists('key.pem'):
        ssl_context = ('cert.pem', 'key.pem')

    app.run(
        host='0.0.0.0',
        port=8000,
        debug=True,
        ssl_context=ssl_context
    )
import os
import json
import pymssql
import argparse
import datetime
import pandas as pd
from dotenv import load_dotenv
from flask_cors import CORS
from flask import Flask, Blueprint, jsonify, request
from waitress import serve

parser = argparse.ArgumentParser()
parser.add_argument("--mode", type = str, help = "development or production mode")
args = parser.parse_args()

# 定義應用程式與 API
app = Flask(__name__)
CORS(app)
apiBp = Blueprint("api", __name__)

# 定義 DataBase
if args.mode == "production":
    SERVER = '10.120.8.231'
    DATABASE = 'HDRPlan' 
    UID = 'sa' 
    PWD = '1qaz@WSX3edc'  
    PORT = '1443'
else:
    SERVER = 'practice-sql-server-wangjianan.database.windows.net'
    DATABASE = 'HDRTest' 
    UID = 'cdc-practice-wangjianan' 
    PWD = '1qaz@WSX3edc'  
    PORT = '1443'

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

def delete_data_from_prpcess_method(
    id: int
) -> str:
    
    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute("""
DELETE FROM dbo.process_method WHERE id = {}
""".format(id))

    return 'success,delete'

def insert_data_to_process_method(
    bpcrtno: str,
    selectedOptions: str,
    others: str,
    id: int = None
) -> str:

    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    if id:
        max_id = id
    else:
        with conn.cursor() as cur:
            cur.execute("""
    SELECT
        MAX(id) as max_id
    FROM 
        dbo.process_method
    """)
            max_id = cur.fetchall()[0]["max_id"]

    now_time = datetime.datetime.now()
    
    insert_text = "({}, '{}', {}, {}, '{}', '{}')".format(
        max_id,
        bpcrtno,
        now_time,
        now_time,
        selectedOptions,
        others
    )

    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute("""
INSERT TABLE dbo.process_method (id, bpcrtno, createdat, updateat, selectionOptions, others)
VALUE 
{}
""".format(insert_text))
        cur.commit()
    return 'You had inserted data: bpcrtno = [{}], selectOptions = [{}], others = [{}], inserted successfully'.format(bpcrtno, selectedOptions, others)

def query_many_process_method(
    fetch: int
):
    
    if fetch <= 20:
        offset = 0
    else:
        offset = fetch - 20

    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute("""
SELECT *
FROM dbo.process_method
ORDER BY createdat DESC
OFFSET {} ROWS
FETCH NEXT 20 ROWS ONLY
""".format(offset))
        data = cur.fetchall()
    return data

def query_process_method(
    id: int,
    bpcrtno: str = None
) -> list:
    
    if bpcrtno:
        id_bpcrtno_text = f"{id} OR bpcrtno = '{bpcrtno}'"
    else:
        id_bpcrtno_text = f"{id}"
    
    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        print(id_bpcrtno_text)
        cur.execute("""
SELECT
    id,
    bpcrtno,
    createdat,
    updateat,
    selectedoptions,
    others
FROM 
    dbo.process_method
WHERE 
    id = {}
ORDER BY 
    createdat DESC
""".format(id_bpcrtno_text))
        data = cur.fetchall()

    return data

def query_all_hmbed(
    tw_date, 
    hmbed_id: str
):
    
    query_text = """
SELECT
    *
FROM (
    SELECT 
        Hmbed AS hmbed,
        no,
        SUBSTRING(
            BPcrtno,
            PATINDEX('%[^0]%', BPcrtno),
            LEN(BPcrtno)
        ) AS bpcrtno,
        Daidate,
        Daitime,
        Systole,
        sort
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(
                PARTITION BY BPDT.Hmbed
                ORDER BY 
                    BPDT.Daidate DESC,
                    BPDT.Daitime ASC
            ) AS sort
        FROM dbo.BPDT
		WHERE 
			SUBSTRING(HmBed, 1, 1) LIKE '{}'
    ) AS BPDT
    WHERE 
		sort = 1
) AS BPDT
INNER JOIN (
    SELECT 
        Y_pred_next_systole,
        Y_pred_Hypo_90mmHg,
        SUBSTRING(
            hmpcrtno,
            PATINDEX('%[^0]%', hmpcrtno),
            LEN(hmpcrtno)
        ) AS hmpcrtno,
        day_stamp,
        predict_daitime,
        measurement_index,
        sort_pre
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY predt.hmpcrtno
                ORDER BY 
                    predt.day_stamp DESC,
                    predt.measurement_index DESC
            ) AS sort_pre
        FROM predt
    ) AS predt
    WHERE sort_pre = 1
) AS predt ON BPDT.bpcrtno = predt.hmpcrtno
WHERE 
    BPDT.Daidate = '{}'
ORDER BY BPDT.hmbed
    """.format(
        hmbed_id,
        tw_date
    )

    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute(query_text)
        data = cur.fetchall()
    return data

def query_info(crtno):

    query_text = """
    SELECT TOP 1
        PatientDT.crtno,
        PatientDT.sex,
        PatientDT.age,
        BPDT.Hmbed
    FROM (
        SELECT
            SUBSTRING(
                crtno,
                PATINDEX('%[^0]%', crtno),
                LEN(crtno)
            ) AS crtno,
            sex,
            113 - CAST(SUBSTRING(birdt, 1, 3) AS INT) AS age
        FROM 
            PatientDT
    ) AS PatientDT
    INNER JOIN (
        SELECT 
            SUBSTRING(
                BPcrtno,
                PATINDEX('%[^0]%', BPcrtno),
                LEN(BPcrtno)
            ) AS BPcrtno,
            Hmbed,
            CAST(Daidate AS INT) AS Daidate,
            CAST(Daitime AS INT) AS Daitime
        FROM 
            BPDT
    ) BPDT ON PatientDT.crtno = BPDT.BPcrtno
    WHERE PatientDT.crtno = '{}'
    ORDER BY 
        BPDT.Daidate,
        BPDT.Daitime
    """.format(crtno)

    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute(query_text)
        data = cur.fetchall()
    return data

def query_bpdt(
    bpcrtno: int | str
) -> list:
    
    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
            cur.execute("""
    SELECT TOP 24 
        daidate,
        daitime,
        systole, 
        diastole, 
        plus, 
        map 
    FROM BPDT
    WHERE bpcrtno LIKE '%{}' 
    ORDER BY daidate DESC, daitime DESC
    """.format(bpcrtno))
            data = cur.fetchall()
    return data

def query_machinedt(
    mdtcrtno: int | str,
    tw_date
) -> list:

    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute(
        """
        SELECT TOP 10
            mdtcrtno, 
            daidate, 
            daitime, 
            vpressure, 
            flowrate, 
            currflowrate, 
            ufrate, 
            uftarget, 
            ufvolume, 
            uftime
        FROM 
            machinedt 
        WHERE 
            mdtcrtno LIKE '%{}' and daidate = '{}'
        """.format(
            mdtcrtno,
            tw_date
        )
        )
        data = cur.fetchall()
    return data

def query_predt(
    hmpcrtno: int | str
):
    
    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute("""
SELECT DISTINCT TOP 24 
    CONVERT(INT, measurement_index) AS measurement_index, 
    hmpcrtno, 
    day_stamp,
    previous_daitime, 
    predict_daitime,  
    y_pred_Hypo_90mmHg AS y_pred_hypo_90mmhg, 
    previous_systole, 
    Y_pred_next_systole AS y_pred_next_systole 
FROM predt 
WHERE hmpcrtno LIKE '%{}'
ORDER BY 
    day_stamp DESC , 
    measurement_index 
DESC 
""".format(hmpcrtno)
)
        data = cur.fetchall()
    return data

def query_nurdt(
    ndcrtno: int | str
):

    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute("""
SELECT TOP 13
    hemvsdt,
                    dw,
                    pre_bw_net,
                    uf_set_machine,
                    post_bw_net, 
                    uf_actual
FROM 
                    nurdt
WHERE ndcrtno LIKE '%{}'
ORDER BY hemvsdt DESC
""".format(
    ndcrtno
))
        data = cur.fetchall()
    return data

def query_best_ufrate(
    tw_date: str,
    area: str
):
    
    query_text = """
SELECT 
    *
FROM (
    SELECT 
        SUBSTRING(mdtcrtno, PATINDEX('%[^0]%', mdtcrtno), LEN(mdtcrtno)) AS mdtcrtno, 
        UFrate,
        UFtime AS uftime,
        Hmbed AS hmbed,
        Daidate,
        Daitime,
        sort 
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(
                PARTITION BY Hmbed
                ORDER BY 
                    Daidate DESC, 
                    Daitime DESC
            ) AS sort
        FROM machinedt
        WHERE Daidate = '{}'
    ) AS machinedt
    WHERE sort = 1
) AS machinedt
INNER JOIN (
    SELECT 
        SUBSTRING(
            Ndcrtno,
            PATINDEX('%[^0]%', Ndcrtno),
            LEN(Ndcrtno)
        ) AS Ndcrtno,
        RTRIM(Dw) AS Dw,
        sort
    FROM (
        SELECT 
            *, 
            ROW_NUMBER() OVER(
                PARTITION BY Ndcrtno
                ORDER BY hemvsdt DESC
            ) AS sort
        FROM nurdt
    ) nurdt
    WHERE sort = 1
) AS nurdt ON machinedt.mdtcrtno = nurdt.Ndcrtno
WHERE 
    machinedt.hmbed LIKE '%{}%' AND
    CAST(machinedt.uftime AS INT) >= 6
ORDER BY 
    machinedt.hmbed
    """.format(
        tw_date,
        area
    )

    conn = pymssql.connect(
        server = SERVER,
        user = UID,
        password = PWD,
        database = DATABASE,
        as_dict = True
    )

    with conn.cursor() as cur:
        cur.execute(query_text)
        data = cur.fetchall()
    return data

def now_datetime_to_roc_datetime_str(
    now_datetime
):
    
    year = str(now_datetime.year - 1911).zfill(3)
    month = str(now_datetime.month).zfill(2)
    day = str(now_datetime.day).zfill(2)
    return f"{year}{month}{day}"

def convert_row_dict_to_column_dict(
    data: list
) -> dict:
    
    data = pd.DataFrame.from_records(data)
    data = data.rename(
        columns = {
            i: i.lower()
            for i in data.columns
        }
    )
    data = data.to_dict("list")
    return data

@apiBp.route("/showall/<hmbed_id>", methods = ["GET"])
def show_all_hmbed_view(
    hmbed_id
):
    
    if args.mode == "production":
        now_datetime = datetime.datetime.now()
        now_datetime = now_datetime_to_roc_datetime_str(
            now_datetime = now_datetime
        )
    else:
        now_datetime = "1110630"
    
    data = query_all_hmbed(
        tw_date = now_datetime,
        hmbed_id = hmbed_id
    )
    # data = convert_row_dict_to_column_dict(
    #     data = data
    # )
    return jsonify(data)

@apiBp.route("/shownurdt/<ndcrtno>", methods = ["GET"])
def show_nurdt_view(
    ndcrtno: int | str
):
    
    data = query_nurdt(
        ndcrtno = ndcrtno
    )
    # data = convert_row_dict_to_column_dict(
    #     data = data
    # )
    return jsonify(data)

@apiBp.route("/showinfo/<crtno>", methods = ["GET"])
def show_info_view(
    crtno: int | str
):
    
    data = query_info(
        crtno = crtno
    )
    # data = convert_row_dict_to_column_dict(
    #     data = data
    # )
    return jsonify(data)

@apiBp.route("/showbpdt/<bpcrtno>", methods = ["GET"])
def show_Bpdt_view(
    bpcrtno: int | str
):
    
    data = query_bpdt(
        bpcrtno = bpcrtno
    )
    # data = convert_row_dict_to_column_dict(
    #     data = data
    # )
    return jsonify(data)

@apiBp.route("/showmachinedt/<mdtcrtno>", methods = ["GET"])
def show_machinedt_view(
    mdtcrtno: int | str
):
    
    if args.mode == "production":
        now_date = str(datetime.datetime.now().date())
        roc_date = eval(
            "{}{}{}".format(
                eval(now_date[:4]) - 1911,
                now_date[5:7],
                now_date[8:]
            )
        )
    else:
        roc_date = "1110630"
    data = query_machinedt(
        mdtcrtno = mdtcrtno,
        tw_date = roc_date
    )
    # data = convert_row_dict_to_column_dict(
    #     data = data
    # )
    return jsonify(data)

@apiBp.route("/showpred/<hmpcrtno>", methods = ["GET"])
def show_predt_view(
    hmpcrtno: int | str
):
    
    data = query_predt(
        hmpcrtno = hmpcrtno
    )
    # data = convert_row_dict_to_column_dict(
    #     data = data
    # )
    return data

# 待處理
@apiBp.route(
    '/v1/process_method', 
    methods=['GET','POST','PUT','DELETE']
)
def process_method_view():

    if request.method == "GET":
        id = request.args.get("id", None)
        bpcrtno = request.args.get("bpcrtno", None)
        data = query_process_method(
            id = id,
            bpcrtno = bpcrtno
        )
        # data = convert_row_dict_to_column_dict(
        #     data = data
        # )

    elif request.method == "POST":
        process_method_data = json.loads(
            request.get_data()
        )
        bpcrtno = process_method_data["bpcrtno"]
        selectedOptions = process_method_data["selectionOptions"]
        others = process_method_data["others"]
        data = insert_data_to_process_method(
            bpcrtno = bpcrtno,
            selectedOptions = selectedOptions,
            others = others
        )

    elif request.method == "PUT":
        id = request.args.get("id", None)
        if id:
            process_method_data = query_process_method(
                id = id
            )
            if process_method_data.__len__() > 0:
                delete_data_from_prpcess_method(
                    id = id
                )

                bpcrtno = process_method_data[0]["bpcrtno"]

                process_method_data = json.loads(request.get_data())
                selectedOptions = process_method_data["selectionOptions"]
                others = process_method_data["others"]
                data = insert_data_to_process_method(
                    id = id,
                    bpcrtno = bpcrtno,
                    selectedOptions = selectedOptions,
                    others = others
                )
                
            else:
                data = 'error, object is not exist or parms id is null'
        else:
            data = 'error, object is not exist or parms id is null'

    elif request.method == "DELETE":
        id = request.args.get("id", None)
        if id:
            data = delete_data_from_prpcess_method(
                id = id
            )
        else:
            data = 'error, object is not exist or parms id is null'

    return jsonify(data)

@apiBp.route("/v1/process_methods", methods = ["GET"])
def process_methods_view():

    fetch = eval(request.args.get("fetch", None))
    data = query_many_process_method(
        fetch = fetch
    )
    # data = convert_row_dict_to_column_dict(
    #     data = data
    # )
    return jsonify(data)

@apiBp.route("/v1/bestufrate/<area>", methods = ["GET"])
def best_uf_rate(area):

    def convert_keys2lower(row):
        keys = [i.lower() for i in row[0].keys()]
        return keys

    # This function for Dry Weight pre-processing
    def dw_preprocessing(one_dw): 
        one_dw = one_dw.replace("..", ".").replace("(", "").replace(")", "").replace(" ", "")
        one_dw = one_dw.split(">")[-1] if ">" in one_dw else one_dw.split(">")[-1]
        after_preprocessing_dw = one_dw[:one_dw.find("(")] if "(" in one_dw else one_dw 
        return str(after_preprocessing_dw) # 

    if args.mode == "production":
        now_date = str(datetime.datetime.now().date())
        roc_date = eval(
            "{}{}{}".format(
                eval(now_date[:4]) - 1911,
                now_date[5:7],
                now_date[8:]
            )
        )
    else:
        roc_date = "1110630"

    data = query_best_ufrate(
        tw_date = roc_date,
        area = area
    )
    data = pd.DataFrame.from_records(data)
    data = data.rename(
        columns = {
            i: i.lower()
            for i in data.columns.tolist()
        }
    )
    data["dw"] = data["dw"].apply(dw_preprocessing).copy().astype("float")
    data["ufr"] = data["ufrate"].astype("float")
    data["best_ufr"] = data["ufr"] * 1000 / data["dw"]
    data["dailtime"] = data["daitime"].copy()
    data["crtno"] = data["mdtcrtno"].copy()
    data = data[["hmbed", "crtno", "best_ufr", "ufr", "dw", "dailtime"]].to_dict("records")
    return jsonify(data)

app.register_blueprint(apiBp, url_prefix = "/api")
if __name__ == "__main__":
    # print("開始運行")
    if args.mode == "development":
        app.run(port = 8089)
    else:
        serve(app = app, port = 8089)
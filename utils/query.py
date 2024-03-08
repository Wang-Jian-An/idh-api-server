import pymssql
import datetime
from flask_server import args

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
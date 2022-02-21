from datetime import datetime,timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'Steven Porras',
    'depends_on_past': False,
    'email': ['steven.porras@treinta.co','mario.callejas@treinta.co'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2021, 1, 1),
}

with DAG( 
    
    dag_id = 'pagos_disp_report_disp_v2',
    schedule_interval = '0 3 * * *',
    default_args = default_args,
    catchup = False,
    tags = ['sandbox', 'payments']

) as dag:

    start = DummyOperator(task_id="start")

    query = """
    select distinct
        p.id                                                                        as payment_id
        ,p.country_id
        ,p.store_id
        ,ser.user_id
        ,s.name                                                                     as store_name
        ,json_value(s.payments_data, '$.confirmIncome')                             as kyc_validation
        ,p.transaction_id
        ,p.payment_type_id
        ,pr.name                                                                    as provider
        ,p.payment_status_id
        ,ps.name                                                                    as payment_status_name
        ,p.payment_method_id
        ,pm.name                                                                    as payment_method_name
        ,p.link
        ,p.value
        ,datetime(timestamp(p.created_at), 'America/Bogota')                        as created_at
        ,datetime(timestamp(p.updated_at), 'America/Bogota')                        as updated_at
        ,p.expired_at
        ,p.user_data
        ,case 
            when el.created_at < sd2.updated_at then datetime(timestamp(sd2.updated_at), 'America/Bogota')
            else datetime(timestamp(el.created_at), 'America/Bogota') end           as diario
        ,left(concat(u.first_name, ' ', u.last_name), 30)                           as owner
        ,dt.name                                                                    as document
        ,u.document                                                                 as document_id
        ,u.document_type_id                                                         as document_type
        ,ds.name                                                                    as document_status
        ,json_value(srv.config, '$.accountNumber')                                  as account
        ,bc.code_dispersion                                                         as bank_code
        ,json_value(srv.config, '$.accountType')                                    as account_type
        ,if(json_value(srv.config, '$.accountType') = '1', '7', json_value(srv.config, '$.accountType')) as meta_account_type
        ,case
            when json_value(srv.config, '$.accountType') = '1' then 37         
            when json_value(srv.config, '$.accountType') = '2' then 27
            end                                                                     as transaction_type
        ,el.amount                                                                  as valor_trx_aceptada
        ,el.amount / (1+0)                                                          as base_antes_impuestos
        ,el.amount - (el.amount)                                                    as iva
        ,el.amount * r.retefuente                                                   as retefuente
        ,el.amount * r.reteica                                                      as reteica
        ,el.amount * r.reteiva                                                      as reteiva
        ,el.amount * c.percentage                                                   as commission
        ,(el.amount * c.percentage) * t.percentage                                  as iva_commission
        ,o.name                                                                     as origin_id
        ,el.amount - (el.amount * r.retefuente) - (el.amount * r.reteica) - (el.amount * r.reteiva) - (el.amount * c.percentage) - ((el.amount * c.percentage) * t.percentage)  as desembolso_trx
    from `treintaco-lz.app_payments.payments` p
        left join `treintaco-lz.app_treinta.store_employee_registers` ser     --store_employee_registers
            on p.store_id = ser.store_id
            and ser.user_type_id = 1
            
        left join `treintaco-lz.app_treinta.stores` s                         --stores
            on ser.store_id = s.id
            
        left join `treintaco-lz.app_treinta.services` srv                     --services
            on s.id = srv.store_id
            and srv.service_type_id = 2
            
        left join `treintaco-lz.app_payments.store_documents` sd
            on s.id = sd.store_id
        left join ((select id, name from `treintaco-lz.app_payments.documents_status` where id!=1
                union all 
                    (select 5 as id, "Enviados" as name))
                union all 
                    (select 1 as id, "No ha finalizado el envío" as name)
                ) ds
            on sd.status_id = ds.id
        left join(
                    select
                        *
                    from  `treintaco-lz.app_payments.store_documents`
                    where status_id = 2
                    qualify row_number() over(partition by id order by updated_at desc) = 1
                ) sd2                                       --store_documents validacion diario
            on s.id = sd2.store_id
        left join `treintaco-lz.app_treinta.users` u                          --users
            on ser.user_id = u.id
        left join `treintaco-lz.app_payments.providers` pr                     --providers
            on p.provider_id = pr.id
        left join `treintaco-lz.app_payments.payment_status` ps                --payment_status
            on p.payment_status_id = ps.id
        left join  `treintaco-lz.app_payments.payment_methods` pm               --payment_methods
            on p.payment_method_id = pm.id
        left join `treintaco-lz.app_treinta.document_types` dt                --document_types
            on u.document_type_id = dt.id
        left join `treintaco-sandbox.pagos_params.bank_codes` bc                    --bank_codes
            on cast(json_value(srv.config, '$.bankId') as int64) = bc.bank_id
        left join(                                          --external_logs
                    select 
                        *
                        ,cast(json_value(logger, '$.transaction.amount') as bignumeric) as amount
                        ,json_value(logger, '$.transaction.dev_reference')              as dev_reference
                    from `treintaco-lz.app_payments.external_logs`
                    where 1=1
                        and json_value(logger, '$.transaction.status') = '1'
                    qualify row_number() over(partition by json_value(logger, '$.transaction.dev_reference') order by created_at) = 1
                ) el
            on p.transaction_id = el.dev_reference
        left join  `treintaco-sandbox.pagos_params.taxes` t                          --taxes
            on p.country_id = t.country_id
        left join `treintaco-sandbox.pagos_params.retentions` r                     --retentions
            on p.payment_method_id = r.payment_method_id
            and p.country_id = r.country_id
        left join `treintaco-sandbox.pagos_params.commissions` c                    --commissions
            on p.country_id = c.country_id
        LEFT JOIN `treintaco-lz.app_treinta.transactions` as trx             -- transactions
            on p.transaction_id = trx.id
        LEFT JOIN  `treintaco-lz.app_treinta.origins` as o                    -- origins
            on trx.origin_id = o.id
    where 1=1
        and p.payment_status_id = 1
        --and regexp_contains(json_value(el.logger, '$.transaction.order_description'), r'(?i)(test|prueba|testing)') = false
    """
    location = 'us-west1'

    task = BigQueryInsertJobOperator(
        task_id="pagos_disp_report_disp_v2",
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
                'destinationTable': {
                    'projectId': 'treintaco-sandbox',
                    'datasetId': 'pagos_payments_dispersions',
                    'tableId': 'report_dispersions_V2'
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "write_disposition": 'WRITE_TRUNCATE'
            }
        },
        location=location,
        gcp_conn_id='bigquery_with_gdrive_scope'
    )
    
    start >> task
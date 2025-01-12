function runQueryAndSaveResults() {
  var projectId = 'data-project-access'; ---// Name of Data Project
  var datasetId = 'dataset-id-mart.sales_data'; ---// Dataset ID
  var tableId = 'merchant_details'; ---// Table ID
  var query = `
  CREATE OR REPLACE TABLE  \`dataset-id-mart.sales_data.merchant_details\` as
  WITH
pickup_comfee_setting as (
      select distinct
      a.merchant_id,
      payment_type,
      source_name,
      billing_cutoff,
      ROUND(ifnull(tax_percentage, 0) * 100, 2) AS tax,
      ROUND(ifnull(sharing_percentage, 0) * 100, 2) AS sharing_percentage,
      ROUND(ifnull(service_charge_percentage, 0) * 100, 2) AS service_charge,
      ROUND(ifnull(merchant_fee_percentage, 0) * 100, 2) AS merchant_fee,
      merchant_fixed_fee_amount AS fixed_fee,
      payout_type,
      case when withholding_tax_flag IS TRUE then 1 ELSE 0 end as withholding_tax,
      acquired_by,
      acquiring_provider_name as acquiring_provider
from \`dataset-id-mart.commission_rules.dim_commission_rule\` a
      left join
        (SELECT DISTINCT merchant_id, classification_name from \`dataset-id-mart.sales_data.merchant_universe_reference\`)  c on a.merchant_id = c.merchant_id
      where 1=1
      and source_name IN ('ONLINE_PICKUP')
      AND classification_name IN ("ENTERPRISE", "SMB-MANAGED')
QUALIFY ROW_NUMBER() OVER(PARTITION BY a.merchant_id ORDER BY updated_timestamp DESC) =1
),

delivery_comfee_setting as (
      select distinct
      a.merchant_id,
      payment_type,
      source_name,
      billing_cutoff,
      ROUND(ifnull(tax_percentage, 0) * 100, 2) AS tax,
      ROUND(ifnull(sharing_percentage, 0) * 100, 2) AS sharing_percentage,
      ROUND(ifnull(service_charge_percentage, 0) * 100, 2) AS service_charge,
      ROUND(ifnull(merchant_fee_percentage, 0) * 100, 2) AS merchant_fee,
      merchant_fixed_fee_amount AS fixed_fee,
      payout_type,
      case when withholding_tax_flag IS TRUE then 1 ELSE 0 end as withholding_tax,
      acquired_by,
      acquiring_provider_name as acquiring_provider
from \`dataset-id-mart.commission_rules.dim_commission_rule\` a
      left join
        (SELECT DISTINCT merchant_id, classification_name 
        from \`dataset-id-mart.sales_data.merchant_universe_reference\`) c on a.merchant_id = c.merchant_id
      where 1=1
      and source_name LIKE '%delivery_online%'
      AND classification_name IN ("ENTERPRISE", "SMB-MANAGED')
QUALIFY ROW_NUMBER() OVER(PARTITION BY a.merchant_id ORDER BY updated_timestamp DESC) =1
),

brand_tag as (
SELECT DISTINCT
merchant_id, brand_id, brand_name as brand_tag
FROM \`dataset-id-presentation.brand_data.dimension_instore_merchant\`
),

integration_flag as (
SELECT DISTINCT 
  a.outlet_id,
  AEAD.DECRYPT_STRING(b.outlet_ck, a.outlet_email, a.outlet_id) outlet_email,
  entity et_id,
  integration.product_resto_status_name integration_flag,
  integration.integration_type

FROM \`dataset-id-presentation.merchant_platform.dim_outlet\` a
left join unnest(entity_tag_list) entity
left join unnest(product_integration_partner_list) integration
left join \`dataset-id-presentation.merchant_platform_outlet_keyset.cryptokey\` b on a.outlet_id = b.outlet_id
left join
(SELECT DISTINCT outlet_id, brand_name,classification_name from \`dataset-id-mart.sales_data.merchant_universe_reference\`) c on a.outlet_id = c.outlet_id
WHERE TRUE
AND c.classification_name IN ("ENTERPRISE", "SMB-MANAGED')
QUALIFY ROW_NUMBER() OVER(PARTITION BY a.outlet_id ORDER BY updated_timestamp DESC) =1
),

bank_data as(
SELECT DISTINCT
  a.outlet_id,
  classification_type_name,
  bank_code AS merchant_bank_name,
  AEAD.DECRYPT_STRING(b.outlet_ck, a.bank_account_number, a.outlet_id) outlet_bank_number,
  AEAD.DECRYPT_STRING(b.outlet_ck, a.bank_account_name, a.outlet_id) outlet_bank_name,
FROM \`dataset-id-presentation.merchant_platform.dim_outlet\` a
LEFT JOIN \`dataset-id-presentation.merchant_platform_outlet_keyset.cryptokey\` b
ON a.outlet_id = b.outlet_id
left join
(SELECT DISTINCT outlet_id, brand_name,classification_type_name from \`dataset-id-mart.sales_data.merchant_universe_reference\`) c on a.outlet_id = c.outlet_id
WHERE TRUE
AND c.classification_name IN ("ENTERPRISE", "SMB-MANAGED')
QUALIFY ROW_NUMBER() OVER(PARTITION BY a.outlet_id ORDER BY updated_timestamp DESC) =1
),

outlet AS (
  SELECT
    DISTINCT sf_outlet_id,
    COALESCE(a.outlet_id,x.outlet_id) outlet_id,
    outlet_email,
    et AS entity_id,
    product_restaurant_uuid,
    product_maf_enabled_flag,
    product_maf_auto_accept_flag,
    product_pickup_enabled_flag,
    
  FROM
    \`dataset-id-presentation.sales_platform.dim_outlet\` AS a
  LEFT JOIN (
    SELECT
      DISTINCT a.outlet_id,
      et,
      product_restaurant_uuid,
      product_maf_enabled_flag,
      product_maf_auto_accept_flag,
      product_pickup_enabled_flag,
      AEAD.DECRYPT_STRING(b.outlet_ck, a.outlet_email, a.outlet_id) outlet_email,
    FROM
      \`dataset-id-presentation.merchant_platform.dim_outlet\` a, unnest(entity_tag_list) et
     LEFT JOIN
    \`dataset-id-presentation.merchant_platform_outlet_keyset.cryptokey\` b
  ON
    a.outlet_id = b.outlet_id
    QUALIFY
      ROW_NUMBER() OVER(PARTITION BY outlet_id ORDER BY updated_timestamp DESC) =1 ) x
  ON
    a.outlet_id = x.outlet_id
  LEFT JOIN (
    SELECT
      DISTINCT outlet_id,
      brand_name,
      classification_name
    FROM
      \`dataset-id-mart.sales_data.merchant_universe_reference\`) c
  ON
    a.outlet_id = c.outlet_id
  WHERE
    classification_name IN ("ENTERPRISE", "SMB-MANAGED)
    AND DATE(effective_timestamp, 'Asia/Jakarta')<=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(COALESCE(expired_timestamp,CURRENT_TIMESTAMP()),"Asia/Jakarta")> DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND NOT a.deleted_flag
    AND IFNULL(a.duplicate_flag,FALSE) IS FALSE 
    ),
  
product_outlet AS (
  SELECT
    DISTINCT 
    sf_outlet_id,
    record_type_name_detail,
    billing_group_entity_id,
    parent_id,
    row_number() over(partition by sf_outlet_id,record_type_name_detail order by effective_timestamp desc) as latest_data_rank
  FROM
    \`dataset-id-presentation.sales_platform.dim_account\` AS a
  WHERE
    LOWER(record_type_name_detail) LIKE "%product%outlet%food%"
    AND DATE(effective_timestamp, 'Asia/Jakarta')<=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(COALESCE(expired_timestamp,CURRENT_TIMESTAMP()),"Asia/Jakarta")> DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND IFNULL(deleted_flag,FALSE) IS FALSE
    AND billing_group_entity_id IS NOT NULL ),
  
  billing_group AS (
  SELECT
    DISTINCT contract_id AS consent_id,
    a.sf_account_id,
    sf_account_name,
    parent_entity_id,
    billing_group_entity_id,
    --sf_billing_group_name,
    billing_group_description,
    parent_entity_detail_name,
    SAFE.AEAD.DECRYPT_STRING( cr.sf_account_ck,
      a.billing_group_npwp_number,
      a.sf_account_id) AS bg_npwp_number,
    SAFE.AEAD.DECRYPT_STRING( cr.sf_account_ck,
      a.billing_group_address,
      a.sf_account_id) AS bg_address,
      ROW_NUMBER() OVER(PARTITION BY sf_account_name ORDER BY effective_timestamp DESC) AS latest_data_rank
  FROM
    \`dataset-id-presentation.sales_platform.dim_account\` a
  LEFT JOIN
    \`dataset-id-presentation.sales_platform_account_keyset.cryptokey\` cr
  ON
    a.sf_account_id = cr.sf_account_id
  WHERE
    TRUE
    AND UPPER(record_type_name_detail) = "BILLING GROUP"
    AND service_product_list_name = 'FOOD'
    AND DATE(effective_timestamp, 'Asia/Jakarta')<=DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND DATE(COALESCE(expired_timestamp,CURRENT_TIMESTAMP()),"Asia/Jakarta")> DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    AND IFNULL(deleted_flag, FALSE) IS FALSE ),
  
  Entity AS (
  SELECT
    a.parent_entity_id,
    parent_entity_name,
 FROM \`dataset-id-mart.food.detail_salesforce_restaurant\` a
  WHERE
    TRUE
    AND jakarta_data_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 day) 
    ),

  outlet_final AS (
  SELECT
    DISTINCT 
    a.outlet_id,
    a.sf_outlet_id AS outlet_sf_outlet_id,
    a.entity_id AS sf_entity_id,
    b.sf_outlet_id AS product_outlet_sf_outlet_id,
    b.record_type_name_detail AS product_outlet_sf_record_type_name,
    --b.billing_group_entity_id AS product_outlet_sf_billing_group_name,
    c.sf_account_name AS sf_billing_group_name,
    --c.consent_id AS billing_group_sf_consent_id,
    c.billing_group_entity_id,
    --sf_billing_group_name,
    billing_group_description,
    d.parent_entity_name parent_entity_detail_name,
    --a.billing_group_npwp_number,
    --b.billing_group_address,
    bg_npwp_number,
    bg_address,
    product_restaurant_uuid,
    product_maf_enabled_flag,
    product_maf_auto_accept_flag,
    product_pickup_enabled_flag,

  FROM
    outlet AS a
  LEFT JOIN
    product_outlet AS b
  ON a.sf_outlet_id = b.sf_outlet_id and b.latest_data_rank=1 
  LEFT JOIN
    billing_group AS c
  ON
    c.latest_data_rank=1  AND c.sf_account_name = b.billing_group_entity_id
  LEFT JOIN
    Entity AS d
  ON
    d.parent_entity_id = a.entity_id
  WHERE
    TRUE
    AND LOWER(record_type_name_detail) LIKE "%product%outlet%food%"
  ),

filter_pickup as (SELECT distinct
a.*,
b.outlet_name,
first_restaurant_phone_number,
second_restaurant_phone_number,
outlet_email,
restaurant_location_address,
c.service_area_name,
brand_id,
b.brand_name,
general_group_name,
f.sf_entity_id,
f.parent_entity_detail_name sf_parent_entity_name,
c.active_flag,
c.partner_flag,
brand_tag,
integration_flag,
outlet_sf_outlet_id,
NULL sf_account_name,
f.billing_group_entity_id,
f.sf_billing_group_name,
f.billing_group_description,
f.parent_entity_detail_name billing_group_entity_name,
bg_npwp_number bg_npwp_number,
bg_address,
merchant_bank_name,
outlet_bank_number,
outlet_bank_name,
restaurant_npwp_number,
product_outlet_sf_record_type_name product_code,
product_restaurant_uuid primary_integration_key,
restaurant_id secondary_integration_key,
product_maf_enabled_flag merchant_acceptance_enabled_flag,
product_maf_auto_accept_flag,
product_pickup_enabled_flag,
integration_type,
date_sub(current_date(), interval 1 day) jakarta_update_date

FROM pickup_comfee_setting a
left join \`dataset-id-mart.sales_data.merchant_universe_reference\`  b on a.merchant_id = b.outlet_id
left join \`dataset-id-mart.food.detail_restaurant_profile_reference\` c on a.merchant_id = c.merchant_platform_outlet_id
left join brand_tag d on a.merchant_id = d.merchant_id
left join integration_flag e on a.merchant_id = e.outlet_id
left join outlet_final f on a.merchant_id = f.outlet_id
left join bank_data g on a.merchant_id = g.outlet_id

where true and
(b.classification_type_name = "ENTERPRISE")

--and c.partner_flag IS TRUE and active_flag IS TRUE
),

filter_delivery as (SELECT distinct
a.*,
b.outlet_name,
first_restaurant_phone_number,
second_restaurant_phone_number,
outlet_email,
restaurant_location_address,
c.service_area_name,
brand_id,
b.brand_name,
general_group_name,
f.sf_entity_id,
c.sf_parent_entity_name,
c.active_flag,
c.partner_flag,
brand_tag,
eim_flag,
outlet_sf_outlet_id,
NULL sf_account_name,
f.billing_group_entity_id,
f.sf_billing_group_name,
f.billing_group_description,
f.parent_entity_detail_name billing_group_entity_name,
bg_npwp_number bg_npwp_number,
bg_address,
merchant_bank_name,
outlet_bank_number,
outlet_bank_name,
restaurant_npwp_number,
product_outlet_sf_record_type_name product_code,
product_restaurant_uuid primary_integration_key,
restaurant_id secondary_integration_key,
product_maf_enabled_flag merchant_acceptance_enabled_flag,
product_maf_auto_accept_flag,
product_pickup_enabled_flag,
integration_type,
date_sub(current_date(), interval 1 day) jakarta_update_date

FROM delivery_comfee_setting a
left join \`dataset-id-mart.sales_data.merchant_universe_reference\` b on a.merchant_id = b.outlet_id
left join \`dataset-id-mart.food.detail_restaurant_profile_reference\` c on a.merchant_id = c.merchant_platform_outlet_id
left join brand_tag d on a.merchant_id = d.merchant_id
left join integration_flag e on a.merchant_id = e.outlet_id
left join outlet_final f on a.merchant_id = f.outlet_id
left join bank_number g on a.merchant_id = g.outlet_id

where true and
(b.classification_type_name = "ENTERPRISE")

--and c.partner_flag IS TRUE and active_flag IS TRUE
)

SELECT * from filter_pickup
UNION ALL
SELECT * FROM filter_delivery

`;

  var request = {
    query: query,
    useLegacySql: false,
    destinationTable: {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tableId
    },
    writeDisposition: 'WRITE_TRUNCATE', // Overwrite the table data
    location: 'US' // Specify the dataset location if known
  };

  try {
    var job = BigQuery.Jobs.query(request, projectId);
    var jobId = job.jobReference.jobId;
    // Poll for the job status until it is done
    let status = job.status;
    while (status && status.state !== 'DONE') {
      Utilities.sleep(10000); // Wait for 10 seconds before checking the job status again
      const jobCheck = BigQuery.Jobs.get(projectId, jobId);
      status = jobCheck.status;
    if (status.errorResult) {
        Logger.log('Job failed with error: ' + status.errorResult.message);
        sendEmail(false, status.errorResult.message);
        return;
      }
    }
    
    Logger.log('Query job completed successfully.');
    sendEmail(true); // Success
  } catch (e) {
    Logger.log('Error running query: ' + e.toString());
    sendEmail(false, e.toString()); // Failure with error message
  }
}

function sendEmail(success, errorMessage) {
  var subject, body;
  if (success) {
    subject = "BigQuery Job Completion Notification";
    body = "The BigQuery job has completed successfully.";
  } else {
    subject = "BigQuery Job Failure Notification";
    body = "The BigQuery job failed to complete." + (errorMessage ? " Error: " + errorMessage : "");
  }

  MailApp.sendEmail({
    to: "email@example.com", // Input your email address
    subject: subject,
    body: body,
  });
}

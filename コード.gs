

function exportSumCount() {
  const projectId = 'm2m-core';
  const sheetName = 'output';
  const spreadsheetId = '1gcMyRSPzbvemIVXizp6MMr_cL3H17DbfaeJjPBa3wJo';

  const query = `
WITH
  FirstQuery AS (
    -- 最初のクエリ
WITH
  DateRange AS (
    SELECT 
      DATE(MIN(create_timestamp)) AS start_date,
      DATE(MAX(create_timestamp)) AS end_date
    FROM 
      \`m2m-core.su_wo.rfid_input_code\`
  ),

  Calendar AS (
    SELECT 
      day AS date
    FROM 
      DateRange,
      UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS day
  ),

  exceptDuplication AS (
    WITH
      RankedRecords AS (
        SELECT
          DATE(rfid.create_timestamp) AS date,  -- DATE 型に変換
          mas.commodity_name,
          rfid.barcodeType,
          rfid.barcode,
          rfid.create_timestamp,
          ROW_NUMBER() OVER (
            PARTITION BY 
              DATE(rfid.create_timestamp),
              mas.commodity_name,
              rfid.barcodeType,
              rfid.barcode
            ORDER BY 
              rfid.create_timestamp ASC
          ) AS row_num
        FROM
          \`m2m-core.su_wo.rfid_input_code\` AS rfid
        LEFT JOIN
          \`m2m-core.su_wo.rfid_commodity\` AS mas
        ON
          rfid.barcode = mas.rfid_id
      )
    SELECT
      date,
      commodity_name,
      barcodeType,
      barcode
    FROM
      RankedRecords
    WHERE
      row_num = 1
  ),

  DailyBarcodeCount AS (
    SELECT
      date,
      COUNT(DISTINCT CASE WHEN commodity_name = 'BathMat' AND barcodeType = 1 THEN barcode END) AS BathMat_type_1_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'BathMat' AND barcodeType = 2 THEN barcode END) AS BathMat_type_2_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'BathMat' AND barcodeType = 3 THEN barcode END) AS BathMat_type_3_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'BathMat' AND barcodeType = 4 THEN barcode END) AS BathMat_type_4_count,
      
      COUNT(DISTINCT CASE WHEN commodity_name = 'BathTowel' AND barcodeType = 1 THEN barcode END) AS BathTowel_type_1_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'BathTowel' AND barcodeType = 2 THEN barcode END) AS BathTowel_type_2_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'BathTowel' AND barcodeType = 3 THEN barcode END) AS BathTowel_type_3_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'BathTowel' AND barcodeType = 4 THEN barcode END) AS BathTowel_type_4_count,
      
      COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleDuvetCover' AND barcodeType = 1 THEN barcode END) AS DoubleDuvetCover_type_1_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleDuvetCover' AND barcodeType = 2 THEN barcode END) AS DoubleDuvetCover_type_2_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleDuvetCover' AND barcodeType = 3 THEN barcode END) AS DoubleDuvetCover_type_3_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleDuvetCover' AND barcodeType = 4 THEN barcode END) AS DoubleDuvetCover_type_4_count,
      
      COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleSheets' AND barcodeType = 1 THEN barcode END) AS DoubleSheets_type_1_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleSheets' AND barcodeType = 2 THEN barcode END) AS DoubleSheets_type_2_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleSheets' AND barcodeType = 3 THEN barcode END) AS DoubleSheets_type_3_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleSheets' AND barcodeType = 4 THEN barcode END) AS DoubleSheets_type_4_count,
      
      COUNT(DISTINCT CASE WHEN commodity_name = 'HandTowel' AND barcodeType = 1 THEN barcode END) AS HandTowel_type_1_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'HandTowel' AND barcodeType = 2 THEN barcode END) AS HandTowel_type_2_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'HandTowel' AND barcodeType = 3 THEN barcode END) AS HandTowel_type_3_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'HandTowel' AND barcodeType = 4 THEN barcode END) AS HandTowel_type_4_count,
      
      COUNT(DISTINCT CASE WHEN commodity_name = 'pillowcase' AND barcodeType = 1 THEN barcode END) AS pillowcase_type_1_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'pillowcase' AND barcodeType = 2 THEN barcode END) AS pillowcase_type_2_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'pillowcase' AND barcodeType = 3 THEN barcode END) AS pillowcase_type_3_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'pillowcase' AND barcodeType = 4 THEN barcode END) AS pillowcase_type_4_count,
      
      COUNT(DISTINCT CASE WHEN commodity_name = 'SingleDuvetCover' AND barcodeType = 1 THEN barcode END) AS SingleDuvetCover_type_1_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'SingleDuvetCover' AND barcodeType = 2 THEN barcode END) AS SingleDuvetCover_type_2_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'SingleDuvetCover' AND barcodeType = 3 THEN barcode END) AS SingleDuvetCover_type_3_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'SingleDuvetCover' AND barcodeType = 4 THEN barcode END) AS SingleDuvetCover_type_4_count,
      
      COUNT(DISTINCT CASE WHEN commodity_name = 'singleSheets' AND barcodeType = 1 THEN barcode END) AS singleSheets_type_1_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'singleSheets' AND barcodeType = 2 THEN barcode END) AS singleSheets_type_2_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'singleSheets' AND barcodeType = 3 THEN barcode END) AS singleSheets_type_3_count,
      COUNT(DISTINCT CASE WHEN commodity_name = 'singleSheets' AND barcodeType = 4 THEN barcode END) AS singleSheets_type_4_count

    FROM
      exceptDuplication
    GROUP BY
      date
  ),

  DailyDataWithCalendar AS (
    SELECT 
      cal.date,
      COALESCE(dbc.BathMat_type_1_count, 0) AS BathMat_type_1_count,
      COALESCE(dbc.BathMat_type_2_count, 0) AS BathMat_type_2_count,
      COALESCE(dbc.BathMat_type_3_count, 0) AS BathMat_type_3_count,
      COALESCE(dbc.BathMat_type_4_count, 0) AS BathMat_type_4_count,
      
      COALESCE(dbc.BathTowel_type_1_count, 0) AS BathTowel_type_1_count,
      COALESCE(dbc.BathTowel_type_2_count, 0) AS BathTowel_type_2_count,
      COALESCE(dbc.BathTowel_type_3_count, 0) AS BathTowel_type_3_count,
      COALESCE(dbc.BathTowel_type_4_count, 0) AS BathTowel_type_4_count,
      
      COALESCE(dbc.DoubleDuvetCover_type_1_count, 0) AS DoubleDuvetCover_type_1_count,
      COALESCE(dbc.DoubleDuvetCover_type_2_count, 0) AS DoubleDuvetCover_type_2_count,
      COALESCE(dbc.DoubleDuvetCover_type_3_count, 0) AS DoubleDuvetCover_type_3_count,
      COALESCE(dbc.DoubleDuvetCover_type_4_count, 0) AS DoubleDuvetCover_type_4_count,
      
      COALESCE(dbc.DoubleSheets_type_1_count, 0) AS DoubleSheets_type_1_count,
      COALESCE(dbc.DoubleSheets_type_2_count, 0) AS DoubleSheets_type_2_count,
      COALESCE(dbc.DoubleSheets_type_3_count, 0) AS DoubleSheets_type_3_count,
      COALESCE(dbc.DoubleSheets_type_4_count, 0) AS DoubleSheets_type_4_count,
      
      COALESCE(dbc.HandTowel_type_1_count, 0) AS HandTowel_type_1_count,
      COALESCE(dbc.HandTowel_type_2_count, 0) AS HandTowel_type_2_count,
      COALESCE(dbc.HandTowel_type_3_count, 0) AS HandTowel_type_3_count,
      COALESCE(dbc.HandTowel_type_4_count, 0) AS HandTowel_type_4_count,
      
      COALESCE(dbc.pillowcase_type_1_count, 0) AS pillowcase_type_1_count,
      COALESCE(dbc.pillowcase_type_2_count, 0) AS pillowcase_type_2_count,
      COALESCE(dbc.pillowcase_type_3_count, 0) AS pillowcase_type_3_count,
      COALESCE(dbc.pillowcase_type_4_count, 0) AS pillowcase_type_4_count,
      
      COALESCE(dbc.SingleDuvetCover_type_1_count, 0) AS SingleDuvetCover_type_1_count,
      COALESCE(dbc.SingleDuvetCover_type_2_count, 0) AS SingleDuvetCover_type_2_count,
      COALESCE(dbc.SingleDuvetCover_type_3_count, 0) AS SingleDuvetCover_type_3_count,
      COALESCE(dbc.SingleDuvetCover_type_4_count, 0) AS SingleDuvetCover_type_4_count,
      
      COALESCE(dbc.singleSheets_type_1_count, 0) AS singleSheets_type_1_count,
      COALESCE(dbc.singleSheets_type_2_count, 0) AS singleSheets_type_2_count,
      COALESCE(dbc.singleSheets_type_3_count, 0) AS singleSheets_type_3_count,
      COALESCE(dbc.singleSheets_type_4_count, 0) AS singleSheets_type_4_count

    FROM 
      Calendar AS cal
    LEFT JOIN 
      DailyBarcodeCount AS dbc
    ON 
      cal.date = dbc.date
  )

SELECT
  date,
  SUM(BathMat_type_1_count) OVER (ORDER BY date) AS cumulative_BathMat_type_1_count,
  SUM(BathMat_type_2_count) OVER (ORDER BY date) AS cumulative_BathMat_type_2_count,
  SUM(BathMat_type_3_count) OVER (ORDER BY date) AS cumulative_BathMat_type_3_count,
  SUM(BathMat_type_4_count) OVER (ORDER BY date) AS cumulative_BathMat_type_4_count,
  
  SUM(BathTowel_type_1_count) OVER (ORDER BY date) AS cumulative_BathTowel_type_1_count,
  SUM(BathTowel_type_2_count) OVER (ORDER BY date) AS cumulative_BathTowel_type_2_count,
  SUM(BathTowel_type_3_count) OVER (ORDER BY date) AS cumulative_BathTowel_type_3_count,
  SUM(BathTowel_type_4_count) OVER (ORDER BY date) AS cumulative_BathTowel_type_4_count,
  
  SUM(DoubleDuvetCover_type_1_count) OVER (ORDER BY date) AS cumulative_DoubleDuvetCover_type_1_count,
  SUM(DoubleDuvetCover_type_2_count) OVER (ORDER BY date) AS cumulative_DoubleDuvetCover_type_2_count,
  SUM(DoubleDuvetCover_type_3_count) OVER (ORDER BY date) AS cumulative_DoubleDuvetCover_type_3_count,
  SUM(DoubleDuvetCover_type_4_count) OVER (ORDER BY date) AS cumulative_DoubleDuvetCover_type_4_count,
  
  SUM(DoubleSheets_type_1_count) OVER (ORDER BY date) AS cumulative_DoubleSheets_type_1_count,
  SUM(DoubleSheets_type_2_count) OVER (ORDER BY date) AS cumulative_DoubleSheets_type_2_count,
  SUM(DoubleSheets_type_3_count) OVER (ORDER BY date) AS cumulative_DoubleSheets_type_3_count,
  SUM(DoubleSheets_type_4_count) OVER (ORDER BY date) AS cumulative_DoubleSheets_type_4_count,
  
  SUM(HandTowel_type_1_count) OVER (ORDER BY date) AS cumulative_HandTowel_type_1_count,
  SUM(HandTowel_type_2_count) OVER (ORDER BY date) AS cumulative_HandTowel_type_2_count,
  SUM(HandTowel_type_3_count) OVER (ORDER BY date) AS cumulative_HandTowel_type_3_count,
  SUM(HandTowel_type_4_count) OVER (ORDER BY date) AS cumulative_HandTowel_type_4_count,
  
  SUM(pillowcase_type_1_count) OVER (ORDER BY date) AS cumulative_pillowcase_type_1_count,
  SUM(pillowcase_type_2_count) OVER (ORDER BY date) AS cumulative_pillowcase_type_2_count,
  SUM(pillowcase_type_3_count) OVER (ORDER BY date) AS cumulative_pillowcase_type_3_count,
  SUM(pillowcase_type_4_count) OVER (ORDER BY date) AS cumulative_pillowcase_type_4_count,
  
  SUM(SingleDuvetCover_type_1_count) OVER (ORDER BY date) AS cumulative_SingleDuvetCover_type_1_count,
  SUM(SingleDuvetCover_type_2_count) OVER (ORDER BY date) AS cumulative_SingleDuvetCover_type_2_count,
  SUM(SingleDuvetCover_type_3_count) OVER (ORDER BY date) AS cumulative_SingleDuvetCover_type_3_count,
  SUM(SingleDuvetCover_type_4_count) OVER (ORDER BY date) AS cumulative_SingleDuvetCover_type_4_count,
  
  SUM(singleSheets_type_1_count) OVER (ORDER BY date) AS cumulative_singleSheets_type_1_count,
  SUM(singleSheets_type_2_count) OVER (ORDER BY date) AS cumulative_singleSheets_type_2_count,
  SUM(singleSheets_type_3_count) OVER (ORDER BY date) AS cumulative_singleSheets_type_3_count,
  SUM(singleSheets_type_4_count) OVER (ORDER BY date) AS cumulative_singleSheets_type_4_count

FROM 
  DailyDataWithCalendar
ORDER BY 
  date

  ),

  -- SecondQuery に日付範囲を追加
  SecondQuery AS (
    WITH
      DateRange AS (
        SELECT 
          DATE(MIN(create_timestamp)) AS start_date,
          DATE(MAX(create_timestamp)) AS end_date
        FROM 
          \`m2m-core.su_wo.rfid_input_code\`
      ),
  
      Calendar AS (
        SELECT 
          day AS date
        FROM 
          DateRange,
          UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS day
      ),

      exceptDuplication AS (
        WITH
          RankedRecords AS (
            SELECT
              DATE(rfid.create_timestamp) AS date,
              mas.commodity_name,
              rfid.barcodeType,
              rfid.barcode,
              rfid.create_timestamp,
              ROW_NUMBER() OVER (
                PARTITION BY 
                  mas.commodity_name,
                  rfid.barcodeType,
                  rfid.barcode
                ORDER BY 
                  rfid.create_timestamp ASC
              ) AS row_num
            FROM
              \`m2m-core.su_wo.rfid_input_code\` AS rfid
            LEFT JOIN
              \`m2m-core.su_wo.rfid_commodity\` AS mas
            ON
              rfid.barcode = mas.rfid_id
            WHERE
              rfid.barcodeType = 4
          )
        SELECT
          date,
          commodity_name,
          barcode
        FROM
          RankedRecords
        WHERE
          row_num = 1
      ),

      DailyBarcodeCount AS (
        SELECT
          date,
          COUNT(DISTINCT CASE WHEN commodity_name = 'BathMat' THEN barcode END) AS BathMat_count,
          COUNT(DISTINCT CASE WHEN commodity_name = 'BathTowel' THEN barcode END) AS BathTowel_count,
          COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleDuvetCover' THEN barcode END) AS DoubleDuvetCover_count,
          COUNT(DISTINCT CASE WHEN commodity_name = 'DoubleSheets' THEN barcode END) AS DoubleSheets_count,
          COUNT(DISTINCT CASE WHEN commodity_name = 'HandTowel' THEN barcode END) AS HandTowel_count,
          COUNT(DISTINCT CASE WHEN commodity_name = 'pillowcase' THEN barcode END) AS pillowcase_count,
          COUNT(DISTINCT CASE WHEN commodity_name = 'SingleDuvetCover' THEN barcode END) AS SingleDuvetCover_count,
          COUNT(DISTINCT CASE WHEN commodity_name = 'singleSheets' THEN barcode END) AS singleSheets_count
        FROM
          exceptDuplication
        GROUP BY
          date
      ),

      DailyDataWithCalendar AS (
        SELECT
          cal.date,
          COALESCE(dbc.BathMat_count, 0) AS BathMat_count,
          COALESCE(dbc.BathTowel_count, 0) AS BathTowel_count,
          COALESCE(dbc.DoubleDuvetCover_count, 0) AS DoubleDuvetCover_count,
          COALESCE(dbc.DoubleSheets_count, 0) AS DoubleSheets_count,
          COALESCE(dbc.HandTowel_count, 0) AS HandTowel_count,
          COALESCE(dbc.pillowcase_count, 0) AS pillowcase_count,
          COALESCE(dbc.SingleDuvetCover_count, 0) AS SingleDuvetCover_count,
          COALESCE(dbc.singleSheets_count, 0) AS singleSheets_count
        FROM
          Calendar AS cal
        LEFT JOIN
          DailyBarcodeCount AS dbc
        ON
          cal.date = dbc.date
      )

    SELECT
      date,
      SUM(BathMat_count) OVER (ORDER BY date) AS cumulative_BathMat_count,
      SUM(BathTowel_count) OVER (ORDER BY date) AS cumulative_BathTowel_count,
      SUM(DoubleDuvetCover_count) OVER (ORDER BY date) AS cumulative_DoubleDuvetCover_count,
      SUM(DoubleSheets_count) OVER (ORDER BY date) AS cumulative_DoubleSheets_count,
      SUM(HandTowel_count) OVER (ORDER BY date) AS cumulative_HandTowel_count,
      SUM(pillowcase_count) OVER (ORDER BY date) AS cumulative_pillowcase_count,
      SUM(SingleDuvetCover_count) OVER (ORDER BY date) AS cumulative_SingleDuvetCover_count,
      SUM(singleSheets_count) OVER (ORDER BY date) AS cumulative_singleSheets_count
    FROM
      DailyDataWithCalendar
  )

-- 最終クエリで日付ごとに結合
SELECT
  FirstQuery.date,
  
  FirstQuery.cumulative_BathMat_type_1_count,
  FirstQuery.cumulative_BathMat_type_2_count,
  FirstQuery.cumulative_BathMat_type_3_count,
  FirstQuery.cumulative_BathMat_type_4_count,
  FirstQuery.cumulative_BathMat_type_1_count - FirstQuery.cumulative_BathMat_type_2_count AS BathMat_status1,
  FirstQuery.cumulative_BathMat_type_2_count - FirstQuery.cumulative_BathMat_type_3_count AS BathMat_status2,
  FirstQuery.cumulative_BathMat_type_3_count - FirstQuery.cumulative_BathMat_type_4_count + SecondQuery.cumulative_BathMat_count AS BathMat_status3,
  SecondQuery.cumulative_BathMat_count AS unique_type_4_BathMat_count,
  (SecondQuery.cumulative_BathMat_count - (
    (FirstQuery.cumulative_BathMat_type_1_count - FirstQuery.cumulative_BathMat_type_2_count) +
    (FirstQuery.cumulative_BathMat_type_2_count - FirstQuery.cumulative_BathMat_type_3_count) +
    (FirstQuery.cumulative_BathMat_type_3_count - FirstQuery.cumulative_BathMat_type_4_count+ SecondQuery.cumulative_BathMat_count)
  )) AS BathMat_quantity_in_stock,
  

  FirstQuery.cumulative_BathTowel_type_1_count,
  FirstQuery.cumulative_BathTowel_type_2_count,
  FirstQuery.cumulative_BathTowel_type_3_count,
  FirstQuery.cumulative_BathTowel_type_4_count,
  FirstQuery.cumulative_BathTowel_type_1_count - FirstQuery.cumulative_BathTowel_type_2_count AS BathTowel_status1,
  FirstQuery.cumulative_BathTowel_type_2_count - FirstQuery.cumulative_BathTowel_type_3_count AS BathTowel_status2,
  FirstQuery.cumulative_BathTowel_type_3_count - FirstQuery.cumulative_BathTowel_type_4_count + SecondQuery.cumulative_BathTowel_count AS BathTowel_status3,
  SecondQuery.cumulative_BathTowel_count AS unique_type_4_BathTowel_count,
  (SecondQuery.cumulative_BathTowel_count - (
    FirstQuery.cumulative_BathTowel_type_1_count - FirstQuery.cumulative_BathTowel_type_2_count +
    FirstQuery.cumulative_BathTowel_type_2_count - FirstQuery.cumulative_BathTowel_type_3_count +
    FirstQuery.cumulative_BathTowel_type_3_count - FirstQuery.cumulative_BathTowel_type_4_count+ SecondQuery.cumulative_BathTowel_count
  )) AS BathTowel_quantity_in_stock,
  

  FirstQuery.cumulative_DoubleDuvetCover_type_1_count,
  FirstQuery.cumulative_DoubleDuvetCover_type_2_count,
  FirstQuery.cumulative_DoubleDuvetCover_type_3_count,
  FirstQuery.cumulative_DoubleDuvetCover_type_4_count,
  FirstQuery.cumulative_DoubleDuvetCover_type_1_count - FirstQuery.cumulative_DoubleDuvetCover_type_2_count AS DoubleDuvetCover_status1,
  FirstQuery.cumulative_DoubleDuvetCover_type_2_count - FirstQuery.cumulative_DoubleDuvetCover_type_3_count AS DoubleDuvetCover_status2,
  FirstQuery.cumulative_DoubleDuvetCover_type_3_count - FirstQuery.cumulative_DoubleDuvetCover_type_4_count + SecondQuery.cumulative_DoubleDuvetCover_count AS DoubleDuvetCover_status3,
  SecondQuery.cumulative_DoubleDuvetCover_count AS unique_type_4_DoubleDuvetCover_count,
  (SecondQuery.cumulative_DoubleDuvetCover_count - (
    FirstQuery.cumulative_DoubleDuvetCover_type_1_count - FirstQuery.cumulative_DoubleDuvetCover_type_2_count +
    FirstQuery.cumulative_DoubleDuvetCover_type_2_count - FirstQuery.cumulative_DoubleDuvetCover_type_3_count +
    FirstQuery.cumulative_DoubleDuvetCover_type_3_count - FirstQuery.cumulative_DoubleDuvetCover_type_4_count+ SecondQuery.cumulative_DoubleDuvetCover_count
  )) AS DoubleDuvetCover_quantity_in_stock,
  

  FirstQuery.cumulative_DoubleSheets_type_1_count,
  FirstQuery.cumulative_DoubleSheets_type_2_count,
  FirstQuery.cumulative_DoubleSheets_type_3_count,
  FirstQuery.cumulative_DoubleSheets_type_4_count,
  FirstQuery.cumulative_DoubleSheets_type_1_count - FirstQuery.cumulative_DoubleSheets_type_2_count AS DoubleSheets_status1,
  FirstQuery.cumulative_DoubleSheets_type_2_count - FirstQuery.cumulative_DoubleSheets_type_3_count AS DoubleSheets_status2,
  FirstQuery.cumulative_DoubleSheets_type_3_count - FirstQuery.cumulative_DoubleSheets_type_4_count + SecondQuery.cumulative_DoubleSheets_count AS DoubleSheets_status3,
  SecondQuery.cumulative_DoubleSheets_count AS unique_type_4_DoubleSheets_count,
  (SecondQuery.cumulative_DoubleSheets_count - (
    FirstQuery.cumulative_DoubleSheets_type_1_count - FirstQuery.cumulative_DoubleSheets_type_2_count +
    FirstQuery.cumulative_DoubleSheets_type_2_count - FirstQuery.cumulative_DoubleSheets_type_3_count +
    FirstQuery.cumulative_DoubleSheets_type_3_count - FirstQuery.cumulative_DoubleSheets_type_4_count + SecondQuery.cumulative_DoubleSheets_count
  )) AS DoubleSheets_quantity_in_stock,

  
  FirstQuery.cumulative_HandTowel_type_1_count,
  FirstQuery.cumulative_HandTowel_type_2_count,
  FirstQuery.cumulative_HandTowel_type_3_count,
  FirstQuery.cumulative_HandTowel_type_4_count,
  FirstQuery.cumulative_HandTowel_type_1_count - FirstQuery.cumulative_HandTowel_type_2_count AS HandTowel_status1,
  FirstQuery.cumulative_HandTowel_type_2_count - FirstQuery.cumulative_HandTowel_type_3_count AS HandTowel_status2,
  FirstQuery.cumulative_HandTowel_type_3_count - FirstQuery.cumulative_HandTowel_type_4_count + SecondQuery.cumulative_HandTowel_count AS HandTowel_status3,
  SecondQuery.cumulative_HandTowel_count AS unique_type_4_HandTowel_count,
  (SecondQuery.cumulative_HandTowel_count - (
    FirstQuery.cumulative_HandTowel_type_1_count - FirstQuery.cumulative_HandTowel_type_2_count +
    FirstQuery.cumulative_HandTowel_type_2_count - FirstQuery.cumulative_HandTowel_type_3_count +
    FirstQuery.cumulative_HandTowel_type_3_count - FirstQuery.cumulative_HandTowel_type_4_count + SecondQuery.cumulative_HandTowel_count
  )) AS HandTowel_quantity_in_stock,

  
  FirstQuery.cumulative_pillowcase_type_1_count,
  FirstQuery.cumulative_pillowcase_type_2_count,
  FirstQuery.cumulative_pillowcase_type_3_count,
  FirstQuery.cumulative_pillowcase_type_4_count,
  FirstQuery.cumulative_pillowcase_type_1_count - FirstQuery.cumulative_pillowcase_type_2_count AS pillowcase_status1,
  FirstQuery.cumulative_pillowcase_type_2_count - FirstQuery.cumulative_pillowcase_type_3_count AS pillowcase_status2,
  FirstQuery.cumulative_pillowcase_type_3_count - FirstQuery.cumulative_pillowcase_type_4_count + SecondQuery.cumulative_pillowcase_count AS pillowcase_status3,
  SecondQuery.cumulative_pillowcase_count AS unique_type_4_pillowcase_count,
  (SecondQuery.cumulative_pillowcase_count - (
    FirstQuery.cumulative_pillowcase_type_1_count - FirstQuery.cumulative_pillowcase_type_2_count +
    FirstQuery.cumulative_pillowcase_type_2_count - FirstQuery.cumulative_pillowcase_type_3_count +
    FirstQuery.cumulative_pillowcase_type_3_count - FirstQuery.cumulative_pillowcase_type_4_count + SecondQuery.cumulative_pillowcase_count
  )) AS pillowcase_quantity_in_stock,

  
  FirstQuery.cumulative_SingleDuvetCover_type_1_count,
  FirstQuery.cumulative_SingleDuvetCover_type_2_count,
  FirstQuery.cumulative_SingleDuvetCover_type_3_count,
  FirstQuery.cumulative_SingleDuvetCover_type_4_count,
  FirstQuery.cumulative_SingleDuvetCover_type_1_count - FirstQuery.cumulative_SingleDuvetCover_type_2_count AS SingleDuvetCover_status1,
  FirstQuery.cumulative_SingleDuvetCover_type_2_count - FirstQuery.cumulative_SingleDuvetCover_type_3_count AS SingleDuvetCover_status2,
  FirstQuery.cumulative_SingleDuvetCover_type_3_count - FirstQuery.cumulative_SingleDuvetCover_type_4_count + SecondQuery.cumulative_SingleDuvetCover_count AS SingleDuvetCover_status3,
  SecondQuery.cumulative_SingleDuvetCover_count AS unique_type_4_SingleDuvetCover_count,
  (SecondQuery.cumulative_SingleDuvetCover_count - (
    FirstQuery.cumulative_SingleDuvetCover_type_1_count - FirstQuery.cumulative_SingleDuvetCover_type_2_count +
    FirstQuery.cumulative_SingleDuvetCover_type_2_count - FirstQuery.cumulative_SingleDuvetCover_type_3_count +
    FirstQuery.cumulative_SingleDuvetCover_type_3_count - FirstQuery.cumulative_SingleDuvetCover_type_4_count + SecondQuery.cumulative_SingleDuvetCover_count
  )) AS SingleDuvetCover_quantity_in_stock,
  
  FirstQuery.cumulative_singleSheets_type_1_count,
  FirstQuery.cumulative_singleSheets_type_2_count,
  FirstQuery.cumulative_singleSheets_type_3_count,
  FirstQuery.cumulative_singleSheets_type_4_count,
  FirstQuery.cumulative_singleSheets_type_1_count - FirstQuery.cumulative_singleSheets_type_2_count AS singleSheets_status1,
  FirstQuery.cumulative_singleSheets_type_2_count - FirstQuery.cumulative_singleSheets_type_3_count AS singleSheets_status2,
  FirstQuery.cumulative_singleSheets_type_3_count - FirstQuery.cumulative_singleSheets_type_4_count + SecondQuery.cumulative_singleSheets_count AS singleSheets_status3,
    SecondQuery.cumulative_singleSheets_count AS unique_type_4_singleSheets_count,
  (SecondQuery.cumulative_singleSheets_count - (
    FirstQuery.cumulative_singleSheets_type_1_count - FirstQuery.cumulative_singleSheets_type_2_count +
    FirstQuery.cumulative_singleSheets_type_2_count - FirstQuery.cumulative_singleSheets_type_3_count +
    FirstQuery.cumulative_singleSheets_type_3_count - FirstQuery.cumulative_singleSheets_type_4_count + SecondQuery.cumulative_singleSheets_count
  )) AS singleSheets_quantity_in_stock
  
  


FROM
  FirstQuery
LEFT OUTER JOIN
  SecondQuery
ON
  FirstQuery.date = SecondQuery.date
ORDER BY
  FirstQuery.date;
  `;

  const results = BigQuery.Jobs.query(
    {
      query: query,
      useLegacySql: false
    },
    projectId
  );

  const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
  let sheet = spreadsheet.getSheetByName(sheetName);

  if (!sheet) {
    sheet = spreadsheet.insertSheet(sheetName);
  } else {
    sheet.getRange("A3:BU").clear();
  }

  const rows = results.rows;
  if (!rows) {
    Logger.log('No data found');
    return;
  }

  // ヘッダーを取得
  const headers = results.schema.fields.map(field => field.name);
  sheet.getRange(3, 1, 1, headers.length).setValues([headers]);

  // データをそのまま書き込む
  const data = rows.map(row => row.f.map(cell => cell.v));
  sheet.getRange(4, 1, data.length, data[0].length).setValues(data);
  Logger.log('Data successfully exported to the sheet');
}

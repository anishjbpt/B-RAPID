SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [finance_bi].[Get_Report_InventoryAgingOnDate] @pReportDate [DATE],@pCompanyNo [NVARCHAR](50),@pDays1 [INT],@pDays2 [INT],@pDays3 [INT],@pDays4 [INT],@pDays5 [INT],@pDays6 [INT],@pDays7 [INT],@pDays8 [INT],@pDays9 [INT],@pDays10 [INT],@pProductType [NVARCHAR](4000),@pUserName [NVARCHAR](50),@pPlantNo [NVARCHAR](1000) AS



	DECLARE @Days1 INT
		,@Days2 INT
		,@Days3 INT
		,@Days4 INT
		,@Days5 INT
		,@Days6 INT
		,@Days7 INT
		,@Days8 INT
		,@Days9 INT
		,@Days10 INT
		,@AllowCompany NVARCHAR(20) = ''

	--DECLARE @FromDate INT = CAST(CONVERT(VARCHAR,@pFromDate,112) AS INT), @ToDate INT = CAST(CONVERT(VARCHAR,@pToDate,112) AS INT)

	DECLARE @ReportDate INT = CAST(CONVERT(VARCHAR,@pReportDate,112) AS INT)

	SELECT @AllowCompany = ISNULL(MAX(cuca.CompanyNo), '')
	FROM finance_bi.CnfgUserCompanyAccess cuca
	WHERE cuca.UserName = LOWER(@pUserName)

	IF ((@AllowCompany<>'' AND @AllowCompany=@pCompanyNo) OR @AllowCompany='')
	BEGIN 

		IF OBJECT_ID('tempdb.dbo.#OutboundQuantity') IS NOT NULL
			DROP TABLE #OutboundQuantity

		CREATE TABLE #OutboundQuantity
		WITH (
			DISTRIBUTION = ROUND_ROBIN,
			CLUSTERED COLUMNSTORE INDEX
		) AS		
		SELECT 							
			fm.[CompanyKey]
			,fm.[PlantKey]
			--1 [CompanyKey]
			--,1 [PlantKey]
			,fm.[ProductKey]
			,fm.[SpecialStock]
			,SUM(CASE WHEN fm.[PostingDateKey] <= @ReportDate THEN fm.[Quantity] ELSE NULL END) AS [Out Stock Quantity]
		FROM [finance_bi].FactMovement fm
		JOIN finance_bi.[DimProduct] dp
			ON dp.[ProductKey]=fm.[ProductKey]
		WHERE fm.[PostingDateKey]<=@ReportDate
		--AND fm.[SpecialStock] NOT IN ('K','W')
		--AND fm.[MovementType] NOT IN ('313','314','315','316','103','104','305','306')
		--AND NOT (fm.ConsumptionPosting IN ('A','V') AND fm.MovementIndicator='B')
		AND fm.[SkipForInventory]=0
		AND fm.[DebitCredit]='H'
		AND fm.[NonZeroDocument]=1
		--AND NOT (dp.[BatchManaged]=0 AND fm.[MovementType] = '321')
		--AND NOT (fm.[MovementType] = '321' AND fm.[NonZeroDocument]=0) --322
	
		GROUP BY
			fm.[CompanyKey]
			,fm.[PlantKey]
			,fm.[ProductKey]
			,fm.[SpecialStock]

		--SELECT * FROM #OutboundQuantity WHERE ProductKey='000000000006900006'


		--IF 1=2 BEGIN


		IF OBJECT_ID('tempdb.dbo.#InventoryAging') IS NOT NULL
		DROP TABLE finance_bi.#InventoryAging

		CREATE TABLE #InventoryAging
		WITH (
			DISTRIBUTION = ROUND_ROBIN,
			CLUSTERED COLUMNSTORE INDEX
		) AS
		SELECT
			nonbatch.[CompanyKey]
		   ,nonbatch.[PlantKey]
		   ,nonbatch.[ProductKey]
		   ,nonbatch.[BaseUOM]
		   ,nonbatch.[AgingDateKey]
		   ,SUM(nonbatch.[Stock Quantity]) [Stock Quantity]
		   ,SUM(nonbatch.[Stock AmountLC]) [Stock AmountLC]
		   --,nonbatch.[Product Price]
		   ,nonbatch.[GL AccountNo]
		   ,nonbatch.[GL AccountName]
		   ,nonbatch.[ProfitCenter]
		   ,nonbatch.[ProfitCenterName]
		   --,nonbatch.[SpecialStock]
		   --,nonbatch.[SalesOrderValStockNo]
		   --,nonbatch.[SalesOrderValStockLineNo]

		FROM (
			SELECT
				inv.[CompanyKey]
				,inv.[PlantKey]
				--,inv.[StorageLocation]
				,inv.[ProductKey]
				,inv.[BaseUOM]
				,inv.[AgingDateKey]
				,CASE WHEN inv.[Cumulative Stock Quantity]-inv.[Out Stock Quantity]>=inv.[Stock Quantity] THEN [Stock Quantity] ELSE inv.[Cumulative Stock Quantity]-inv.[Out Stock Quantity] END [Stock Quantity]
				,CASE WHEN inv.[Cumulative Stock Quantity]-inv.[Out Stock Quantity]>=inv.[Stock Quantity] THEN [Stock Quantity] ELSE inv.[Cumulative Stock Quantity]-inv.[Out Stock Quantity] END * inv.[Product Price] AS [Stock AmountLC]
				,inv.[Product Price] 
				,inv.[GL AccountNo]
				,inv.[GL AccountName]
				,inv.[SpecialStock]
				,inv.[SalesOrderValStockNo]
				,inv.[SalesOrderValStockLineNo]
				,inv.[ProfitCenter]
				,inv.[ProfitCenterName]
			FROM (
		
					SELECT
						inv.[CompanyKey]
					   ,inv.[PlantKey]
					   --,inv.[StorageLocation]
					   ,inv.[ProductKey]
					   ,inv.[BaseUOM]
					   ,inv.[AgingDateKey]
					   ,inv.[Stock Quantity]
					   ,inv.[Stock AmountLC]
					   ,SUM(inv.[Stock Quantity]) OVER(PARTITION BY inv.[CompanyKey],inv.[PlantKey],inv.[ProductKey],inv.[SpecialStock] ORDER BY inv.[AgingDateKey],ISNULL(inv.[SalesOrderValStockNo],999999999999) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS [Cumulative Stock Quantity]
					   ,ISNULL(ABS(o.[Out Stock Quantity]),0) AS [Out Stock Quantity]
					   ,inv.[Product Price]
					   ,inv.[GL AccountNo]
					   ,inv.[GL AccountName]
					   ,inv.[SpecialStock]
					   ,inv.[SalesOrderValStockNo]
					   ,inv.[SalesOrderValStockLineNo]
					   ,inv.[ProfitCenter]
					   ,inv.[ProfitCenterName]
					FROM (
							SELECT
								inv.[CompanyKey]
							   ,inv.[PlantKey]
							   --,inv.[StorageLocation]
							   ,inv.[ProductKey]
							   ,inv.[BaseUOM]
							   --,inv.PostingDateKey
							   --,inv.ProductionBatchDateKey
							   --,inv.DocumentNo
							   ,inv.[PostingDateKey] AS [AgingDateKey]
							   ,SUM(inv.[Stock Quantity]) [Stock Quantity]
							   ,SUM(inv.[Stock Quantity]*inv.[Product Price]) [Stock AmountLC]
							   ,inv.[Product Price]
							   ,inv.[GL AccountNo]
							   ,inv.[GL AccountName]
							   ,inv.[SpecialStock]
							   ,inv.[SalesOrderValStockNo]
							   ,inv.[SalesOrderValStockLineNo]
							   ,inv.[ProfitCenter]
							   ,inv.[ProfitCenterName]
							FROM (
									SELECT 							
										fm.[CompanyKey]
										,fm.[PlantKey]
										--1 AS [CompanyKey]
										--,1 AS [PlantKey]
										--,fm.[StorageLocation]
										,fm.[ProductKey]
										,fm.[BaseUOM]
										,fm.[PostingDateKey]
										--,fm.[DocumentNo]
										--,fm.[FiscalYear]
										--,fm.[MovementType]
										--,fm.[SpecialStock]
										,SUM(CASE WHEN fm.[PostingDateKey] <= @ReportDate THEN fm.[Quantity] ELSE NULL END) AS [Stock Quantity]
										,ISNULL(dsosv.[Price VERPR_STPRS],dmv.[Price VERPR_STPRS]) AS [Product Price]
										,ISNULL(dsosv.[GLAccountNo KONTS],dmv.[GLAccountNo KONTS]) AS [GL AccountNo]
										,ISNULL(dsosv.[GLAccount TXT20],dmv.[GLAccount TXT20]) AS [GL AccountName]
										,fm.[SpecialStock]
										,fm.[SalesOrderValStockNo]
										,fm.[SalesOrderValStockLineNo]
										,NULLIF(fm.[ProfitCenter],0) [ProfitCenter]
										,fm.[ProfitCenterName]
									FROM [finance_bi].[FactMovement] fm
									JOIN finance_bi.[DimProduct] dp
										ON dp.[ProductKey]=fm.[ProductKey]
									JOIN finance_bi.[DimPlant] pl
										ON pl.[PlantKey]=fm.[PlantKey]
									LEFT JOIN finance_bi.[DimSalesOrderStockValuation] dsosv
										ON dsosv.[ProductNo MATNR]=fm.[ProductKey]
										AND dsosv.[PlantNo BWKEY]=pl.[PlantNo]
										AND dsosv.[ValuationType BWTAR]=''
										AND dsosv.[SpecialStock SOBKZ]=fm.[SpecialStock]
										AND dsosv.[SalesOrderNo VBELN]=fm.[SalesOrderValStockNo]
										AND dsosv.[SalesOrderLineNo POSNR]=fm.[SalesOrderValStockLineNo]
										AND @pReportDate BETWEEN dsosv.[StartDate] AND dsosv.[EndDate]
										AND fm.[SpecialStock]<>''
									LEFT JOIN [finance_bi].[DimMaterialValuation] dmv
										ON dmv.[ProductNo MATNR]=fm.[ProductKey]
										AND dmv.[PlantNo BWKEY]=pl.[PlantNo]
										AND dmv.[ValuationType BWTAR]=''
										AND @pReportDate BETWEEN dmv.[StartDate] AND dmv.[EndDate]
									WHERE fm.[PostingDateKey]<=@ReportDate
									--AND dp.ProductNum='2005136'--'4000028'--'2044132'--'603100343'--'2044132'--'4000042'--'401019349'
									--AND fm.DocumentNo='4943232805'
									--AND fm.[SpecialStock] NOT IN ('K','W')
									--AND fm.[MovementType] NOT IN ('313','314','315','316','103','104','305','306')
									--AND NOT (fm.ConsumptionPosting IN ('A','V') AND fm.MovementIndicator='B')
									AND fm.[SkipForInventory]=0
									AND fm.[DebitCredit]='S'
									AND dp.[BatchManaged]=0
									AND fm.[NonZeroDocument]=1
									AND (pl.[PlantNo] IN (SELECT value FROM STRING_SPLIT(@pPlantNo,',')) OR @pPlantNo = 'ALL')
									GROUP BY
										fm.[CompanyKey]
										,fm.[PlantKey]
										--,fm.[StorageLocation]
										,fm.[ProductKey]
										--,dp.[ProductNum]
										,fm.[BaseUOM]
										,fm.[PostingDateKey]
										--,fm.[DocumentNo]
										--,fm.[FiscalYear]
										--,fm.[MovementType]
										--,fm.[SpecialStock]
										,ISNULL(dsosv.[Price VERPR_STPRS],dmv.[Price VERPR_STPRS])
										,ISNULL(dsosv.[GLAccountNo KONTS],dmv.[GLAccountNo KONTS])
										,ISNULL(dsosv.[GLAccount TXT20],dmv.[GLAccount TXT20])
										,fm.[SpecialStock]
										,fm.[SalesOrderValStockNo]
										,fm.[SalesOrderValStockLineNo]
										,NULLIF(fm.[ProfitCenter],0)
										,fm.[ProfitCenterName]
							) inv
							GROUP BY 
								inv.[CompanyKey]
							   ,inv.[PlantKey]
							   --,inv.[StorageLocation]
							   ,inv.[ProductKey]
							   ,inv.[BaseUOM]
							   ,inv.[PostingDateKey]
							   ,inv.[Product Price]
							   ,inv.[GL AccountNo]
							   ,inv.[GL AccountName]
							   ,inv.[SpecialStock]
							   ,inv.[SalesOrderValStockNo]
							   ,inv.[SalesOrderValStockLineNo]
							   ,inv.[ProfitCenter]
							   ,inv.[ProfitCenterName]
					) inv
					LEFT JOIN #OutboundQuantity o
						ON inv.[CompanyKey]=o.[CompanyKey]
						AND inv.[PlantKey]=o.[PlantKey]
						AND inv.[ProductKey]=o.[ProductKey]
						AND inv.[SpecialStock]=o.[SpecialStock]

			) inv
			WHERE CASE WHEN inv.[Cumulative Stock Quantity]-inv.[Out Stock Quantity]>=inv.[Stock Quantity] THEN [Stock Quantity] ELSE inv.[Cumulative Stock Quantity]-inv.[Out Stock Quantity] END > 0
		) nonbatch	
		GROUP BY 
			nonbatch.[CompanyKey]
		   ,nonbatch.[PlantKey]
		   ,nonbatch.[ProductKey]
		   ,nonbatch.[BaseUOM]
		   ,nonbatch.[AgingDateKey]
		   ,nonbatch.[GL AccountNo]
		   ,nonbatch.[GL AccountName]
		   ,nonbatch.[ProfitCenter]
		   ,nonbatch.[ProfitCenterName]

		UNION ALL

		SELECT
			batch.[CompanyKey]
		   ,batch.[PlantKey]
		   ,batch.[ProductKey]
		   ,batch.[BaseUOM]
		   ,batch.[AgingDateKey]
		   ,SUM(batch.[Stock Quantity])
		   ,SUM(batch.[Stock AmountLC])
		   ,batch.[GL AccountNo]
		   ,batch.[GL AccountName]
		   ,batch.[ProfitCenter]
		   ,batch.[ProfitCenterName]
		FROM (
				SELECT
					batch.[CompanyKey]
					,batch.[PlantKey]
					--,batch.[StorageLocation]
					,batch.[ProductKey]
					,batch.[BaseUOM]
					,batch.[AgingDateKey]
					,SUM(batch.[Stock Quantity]) AS [Stock Quantity]
					,SUM(batch.[Stock AmountLC]) AS [Stock AmountLC]
					,SUM(batch.[Stock Quantity]) AS [Cumulative Stock Quantity]
					,0 AS [Out Stock Quantity]
					,batch.[GL AccountNo]
					,batch.[GL AccountName]
					,NULL AS [Product Price]
					,batch.[ProfitCenter]
					,batch.[ProfitCenterName]
				FROM (
						SELECT
							inv.[CompanyKey]
							,inv.[PlantKey]
							--,inv.[StorageLocation]
							,inv.[ProductKey]
							,inv.[BaseUOM]
							--,inv.PostingDateKey
							--,inv.ProductionBatchDateKey
							--,inv.DocumentNo
							,inv.[PostingDateKey] AS [AgingDateKey]
							,SUM(inv.[Stock Quantity]) [Stock Quantity]
							,SUM(inv.[Stock Quantity]*inv.[Product Price]) [Stock AmountLC]
							,inv.[GL AccountNo]
							,inv.[GL AccountName]
							,inv.[ProfitCenter]
							,inv.[ProfitCenterName]
						FROM (
								SELECT 							
									fm.[CompanyKey]
									,fm.[PlantKey]
									--,fm.[StorageLocation]
									,fm.[ProductKey]
									,fm.[BaseUOM]
									,fm.[BatchDateKey] [PostingDateKey]
									--,fm.[DocumentNo]
									--,fm.[FiscalYear]
									--,fm.[MovementType]
									--,fm.[SpecialStock]
									,SUM(CASE WHEN fm.[PostingDateKey] <= @ReportDate THEN fm.[Quantity] ELSE NULL END) AS [Stock Quantity]
									,ISNULL(dsosv.[Price VERPR_STPRS],dmv.[Price VERPR_STPRS]) AS [Product Price]
									,ISNULL(dsosv.[GLAccountNo KONTS],dmv.[GLAccountNo KONTS]) AS [GL AccountNo]
									,ISNULL(dsosv.[GLAccount TXT20],dmv.[GLAccount TXT20]) AS [GL AccountName]
									,NULLIF(fm.ProfitCenter,0) [ProfitCenter]
									,fm.[ProfitCenterName]
								FROM [finance_bi].[FactMovement] fm
								JOIN finance_bi.[DimProduct] dp
									ON dp.[ProductKey]=fm.[ProductKey]
								JOIN finance_bi.[DimPlant] pl
									ON pl.[PlantKey]=fm.[PlantKey]
								LEFT JOIN finance_bi.[DimSalesOrderStockValuation] dsosv
									ON dsosv.[ProductNo MATNR]=fm.[ProductKey]
									AND dsosv.[PlantNo BWKEY]=pl.[PlantNo]
									AND dsosv.[ValuationType BWTAR]=''
									AND dsosv.[SpecialStock SOBKZ]=fm.[SpecialStock]
									AND dsosv.[SalesOrderNo VBELN]=fm.[SalesOrderValStockNo]
									AND dsosv.[SalesOrderLineNo POSNR]=fm.[SalesOrderValStockLineNo]
									AND @pReportDate BETWEEN dsosv.[StartDate] AND dsosv.[EndDate]
									AND fm.[SpecialStock]<>''
								LEFT JOIN [finance_bi].[DimMaterialValuation] dmv
									ON dmv.[ProductNo MATNR]=fm.[ProductKey]
									AND dmv.[PlantNo BWKEY]=pl.[PlantNo]
									AND dmv.[ValuationType BWTAR]=''
									AND @pReportDate BETWEEN dmv.[StartDate] AND dmv.[EndDate]
								WHERE fm.[PostingDateKey]<=@ReportDate
								--AND dp.ProductNum='2005136'--'4000028'--'2044132'--'603100343'--'2044132'--'4000042'--'401019349'
								--AND fm.DocumentNo='4943232805'
								--AND fm.[SpecialStock] NOT IN ('K','W')
								--AND fm.[MovementType] NOT IN ('313','314','315','316','103','104','305','306')
								--AND NOT (fm.ConsumptionPosting IN ('A','V') AND fm.MovementIndicator='B')
								AND fm.[SkipForInventory]=0
								AND dp.[BatchManaged]=1
								AND (pl.[PlantNo] IN (SELECT value FROM STRING_SPLIT(@pPlantNo,',')) OR @pPlantNo = 'ALL')
								GROUP BY
									fm.[CompanyKey]
									,fm.[PlantKey]
									--,fm.[StorageLocation]
									,fm.[ProductKey]
									,fm.[BaseUOM]
									,fm.[BatchDateKey]
									--,fm.[DocumentNo]
									--,fm.[FiscalYear]
									--,fm.[MovementType]
									--,fm.[SpecialStock]
									,ISNULL(dsosv.[Price VERPR_STPRS],dmv.[Price VERPR_STPRS])
									,ISNULL(dsosv.[GLAccountNo KONTS],dmv.[GLAccountNo KONTS])
									,ISNULL(dsosv.[GLAccount TXT20],dmv.[GLAccount TXT20])
									,NULLIF(fm.ProfitCenter,0)
									,fm.[ProfitCenterName]
							) inv
							GROUP BY 
								inv.[CompanyKey]
								,inv.[PlantKey]
								--,inv.[StorageLocation]
								,inv.[ProductKey]
								,inv.[BaseUOM]
								,inv.[PostingDateKey]
								,inv.[GL AccountNo]
								,inv.[GL AccountName]
								,inv.[ProfitCenter]
								,inv.[ProfitCenterName]
					) batch
					GROUP BY 
						batch.[CompanyKey]
						,batch.[PlantKey]
						--,batch.[StorageLocation]
						,batch.[ProductKey]
						,batch.[BaseUOM]
						,batch.[AgingDateKey]
						,batch.[GL AccountNo]
						,batch.[GL AccountName]
						,batch.[ProfitCenter]
						,batch.[ProfitCenterName]
					HAVING ISNULL(SUM(batch.[Stock Quantity]),0) <> 0
			) batch
			GROUP BY 
				batch.[CompanyKey]
			   ,batch.[PlantKey]
			   ,batch.[ProductKey]
			   ,batch.[BaseUOM]
			   ,batch.[AgingDateKey]
			   ,batch.[GL AccountNo]
			   ,batch.[GL AccountName]
			   ,batch.[ProfitCenter]
			   ,batch.[ProfitCenterName]
	--SELECT * FROM #InventoryAging ia WHERE ia.ProductKey='000000000006900610' ORDER BY [AgingDateKey]

	--IF 1=2 BEGIN
		--SELECT 
		--	tmn.val
		--	,ia.*
		--	,DATEDIFF(DAY, dd.[Date], @pReportDate) [Days]
		--FROM #InventoryAging ia 
		--JOIN finance_bi.DimDate dd
		--	ON dd.[DateKey]=ia.[AgingDateKey]	
		--JOIN finance_bi.DimPlant dp
		--	ON dp.PlantKey=ia.PlantKey
		--JOIN finance_bi.DimProduct dp1
		--	ON dp1.ProductKey=ia.ProductKey
		--JOIN finance_bi.tmp1 tmn
		--	ON tmn.num=dp.PlantNo+'_'+dp1.ProductNum
		----WHERE ia.[ProductKey]='000000000401000266' 
		--ORDER BY dp.PlantNo,dp1.ProductNo, [AgingDateKey]
	
		----SELECT * FROM finance_bi.tmp1 tmn

	
	


		SELECT
			@Days1=@pDays1+1
			,@Days2=@pDays2+1
			,@Days3=@pDays3+1
			,@Days4=@pDays4+1
			,@Days5=@pDays5+1
			,@Days6=@pDays6+1
			,@Days7=@pDays7+1
			,@Days8=@pDays8+1
			,@Days9=@pDays9+1
			,@Days10=@pDays10+1




	--SELECT * FROM #InventoryAging WHERE [ProductKey]='000000000608100003' 

	



	--IF 1=2
		SELECT
			inv.[CompanyKey]
			,dc.[CompanyNo]
			,dc.[Name] AS [CompanyName]
		   ,inv.[PlantKey]
		   ,dp.[PlantNo]
		   ,dp.[Name1] AS [PlantName]
		   --,dl.[LocationNo]
		   --,dl.[Name] AS [LocationName]
		   ,NULL AS [LocationNo]
		   ,NULL AS [LocationName]
		   ,inv.[ProductKey]
		   ,dpr.[ProductNum]
		   ,dpr.[ProductName]
		   ,dpr.[ProductType]
		   ,dpr.[ProductTypeName]
		   --,inv.[ProductNum]
		   ,inv.[BaseUOM]
		   --,inv.[PostingDateKey]
		   --,inv.[ProductionBatchDateKey]
		   --,inv.[DocumentNo]
		   --,inv.[MovementType]
		   --,inv.[SpecialStock]
		   --,inv.[BatchManagement]
		 --  ,CASE	
			--	WHEN inv.[Days] BETWEEN 0 AND 15 THEN 'Period_0_15'
			--	WHEN inv.[Days] BETWEEN 16 AND 30 THEN 'Period_16_30'
			--	WHEN inv.[Days] BETWEEN 31 AND 60 THEN 'Period_31_60'
			--	WHEN inv.[Days] BETWEEN 61 AND 90 THEN 'Period_31_61'
			--	WHEN inv.[Days] BETWEEN 91 AND 180 THEN 'Period_91_180'
			--	WHEN inv.[Days] BETWEEN 181 AND 365 THEN 'Period_181_365'
			--	WHEN inv.[Days] BETWEEN 366 AND 730 THEN 'Period_366_730'
			--	WHEN inv.[Days] BETWEEN 731 AND 1095 THEN 'Period_731_1095'
			--	WHEN inv.[Days] BETWEEN 1096 AND 1460 THEN 'Period_1096_1460'
			--	WHEN inv.[Days] BETWEEN 1461 AND 1825 THEN 'Period_1461_1825'
			--	WHEN inv.[Days] >= 1826 THEN '1826 Days More'
			--END AS [Period Description]
		   --,DATEDIFF(DAY, dd.[Date], @pReportDate) [Days]
		   --,inv.[Avg Price]
		   ,SUM(inv.[Stock Quantity]) AS [CurrectStockQty]
		   ,SUM(inv.[Stock AmountLC]) AS [CurrentStockAmnt]
	   
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days1 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod1]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days1 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days2 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod2]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days2 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days3 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod3]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days3 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days4 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod4]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days4 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days5 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod5]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days5 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days6 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod6]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days6 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days7 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod7]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days7 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days8 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod8]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days8 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days9 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod9]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days9 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days10 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod10]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days10 THEN inv.[Stock Quantity] ELSE NULL END) [StockQtyPeriod11]
	   

		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days1 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod1]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days1 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days2 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod2]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days2 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days3 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod3]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days3 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days4 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod4]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days4 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days5 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod5]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days5 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days6 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod6]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days6 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days7 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod7]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days7 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days8 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod8]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days8 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days9 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod9]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days9 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days10 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod10]
		   ,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days10 THEN inv.[Stock AmountLC] ELSE NULL END) [StockAmntPeriod11]

		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days1 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod1]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days1 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days2 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod2]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days2 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days3 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod3]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days3 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days4 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod4]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days4 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days5 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod5]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days5 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days6 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod6]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days6 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days7 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod7]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days7 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days8 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod8]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days8 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days9 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod9]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days9 AND DATEDIFF(DAY, dd.[Date], @pReportDate) < @Days10 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod10]
		   --,SUM(CASE WHEN DATEDIFF(DAY, dd.[Date], @pReportDate) >= @Days10 THEN inv.[Stock Quantity] ELSE NULL END*ISNULL(mb5l.Price, dpp.[Price VERPR_STPRS]) ) [StockAmntPeriod11]

		   ,inv.[GL AccountNo]
		   ,inv.[GL AccountName]
		   ,inv.[ProfitCenter]
		   ,inv.[ProfitCenterName]
		FROM #InventoryAging inv
		JOIN finance_bi.DimDate dd
			ON dd.[DateKey]=inv.[AgingDateKey]
		JOIN finance_bi.DimProduct dpr
			ON dpr.[ProductKey]=inv.[ProductKey]
		LEFT JOIN finance_bi.DimPlant dp
			ON dp.[PlantKey]=inv.[PlantKey]
		LEFT JOIN finance_bi.DimCompany dc
			ON dc.[CompanyKey]=inv.[CompanyKey]

		WHERE (
				(dc.[CompanyNo] = '1001' AND @pCompanyNo = '1001')
				OR (dc.[CompanyNo] IN ('ES10','ES20') AND @pCompanyNo = 'ES10')
				OR (@pCompanyNo='ALL')
				--OR 1=1
			)
		--AND dpr.ProductNum='401019349'--'7000285'--'608100003'
		AND (dpr.ProductType IN (SELECT value FROM STRING_SPLIT(@pProductType,',') ) OR @pProductType = 'ALL' OR @pProductType IS NULL)
		--AND inv.ProductKey='000000000006900610'
		AND (dc.[CompanyNo] = @AllowCompany OR @AllowCompany = '')
		GROUP BY 
			inv.[CompanyKey]
			,dc.[CompanyNo]
			,dc.[Name] 
		   ,inv.[PlantKey]
		   ,dp.[PlantNo]
		   ,dp.[Name1] 
		   --,dl.[LocationNo]
		   --,dl.[Name]
		   ,inv.[ProductKey]
		   ,dpr.[ProductNum]
		   ,dpr.[ProductName]
		   ,dpr.[ProductType]
		   ,dpr.[ProductTypeName]
		   ,inv.[BaseUOM]
		   ,inv.[GL AccountNo]
		   ,inv.[GL AccountName]
		   ,inv.[ProfitCenter]
		   ,inv.[ProfitCenterName]
		HAVING ISNULL(SUM(inv.[Stock Quantity]),0)<>0


	END
GO
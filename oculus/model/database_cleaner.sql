USE [Oculus]
GO

DELETE FROM dbo.gebrauchtwagen_clean;

INSERT INTO [dbo].[gebrauchtwagen_clean]
           ([id]
           ,[make]
           ,[model]
           ,[mileage]
           ,[engine_effect]
           ,[engine_fuel]
           ,[year_model]
           ,[location]
           ,[price])
     SELECT id, make, model, mileage, engine_effect, engine_fuel, year_model, location, price
		FROM dbo.gebrauchtwagen;
GO


UPDATE dbo.gebrauchtwagen_clean
SET year_model = 2024
WHERE (year_model = '' OR year_model IS NULL) AND mileage < 1000;

GO

UPDATE dbo.gebrauchtwagen_clean
SET mileage = 0
WHERE year_model = 2024 AND (mileage = '' );
GO

DELETE FROM dbo.gebrauchtwagen_clean
WHERE 
    model = '' OR
    engine_fuel = '' OR
    year_model = '';
GO
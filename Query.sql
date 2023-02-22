WITH crash_cleansing AS				--Data Cleansing
(
SELECT 	*, 
		CASE
		WHEN state_name IN ('Alabama', 'Arkansas', 'Illinois', 'Iowa', 'Kansas','Louisiana',
						   'Minnesota', 'Mississippi', 'Missouri', 'Nebraska', 'North Dakota',
							'South Dakota', 'Oklahoma','Tennessee','Texas','Wisconsin') 
						THEN timestamp_of_crash at time zone 'CST' --(min 6 jam)
		WHEN state_name IN ('Connecticut', 'Delaware', 'District of Columbia', 'Florida','Georgia',
						   'Indiana', 'Kentucky', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
						   'New Hampshire', 'New Jersey', 'New York', 'North Carolina', 'Ohio','Pennsylvania',
						   'Rhode Island', 'South Carolina', 'Vermont', 'Virginia', 'West Virginia') 
						THEN timestamp_of_crash at time zone 'CDT' --(min 5 jam)
		WHEN state_name IN ('Alaska') THEN timestamp_of_crash at time zone 'AKST' --(min 9 jam)
		WHEN state_name IN ('Hawaii') THEN timestamp_of_crash at time zone 'HST' --(min 10 jam)
		WHEN state_name IN ('Arizona', 'Colorado', 'Idaho', 'Montana', 'New Mexico', 'Utah', 'Wyoming') 
						THEN timestamp_of_crash at time zone 'MST' --(min 7 jam)
		ELSE timestamp_of_crash at time zone 'PST' --(min 8 jam)
		END Waktu_Kejadian_Perkara
FROM 	crash
----------------------------------Cleansing----------------------------------
WHERE 	milepoint NOT IN (99999, 99998)
AND		land_use_name NOT IN ('Unknown', 'Not Reported')
AND		functional_system_name NOT IN ('Unknown', 'Not Reported')
AND		type_of_intersection_name NOT IN ('Not Reported')
AND		atmospheric_conditions_1_name NOT IN ('Not Reported')
AND		city_name NOT IN ('Not Reported', 'Unknown', 'Other')
ORDER BY 1 DESC
),

a AS							--Mencari tanggal akhir data kecelakaan
(
SELECT	Waktu_Kejadian_Perkara last_date
FROM crash_cleansing
ORDER BY 1 DESC
LIMIT 1
),

x AS
(
SELECT	COUNT(*) total_kecelakaan	--Mencari total kecelakaan
FROM crash_cleansing
)

SELECT 	DISTINCT b.d_number nomor_hari_kecelakaan,	--nomer hari digunakan untuk
		(CASE										--menentukan hari kecelakaan
		WHEN b.d_number = 0 THEN 'Jumat'
		WHEN b.d_number = 6 THEN 'Sabtu'
		WHEN b.d_number = 5 THEN 'Minggu'
		WHEN b.d_number = 4 THEN 'Senin'
		WHEN b.d_number = 3 THEN 'Selasa'
		WHEN b.d_number = 2 THEN 'Rabu'
		WHEN b.d_number = 1 THEN 'Kamis'
		END) hari_kecelakaan,
		COUNT(b.consecutive_number) jumlah_kecelakaan
FROM
----------------------------------------Sub Query-------------------------------------------------
(
SELECT 	crash_cleansing.consecutive_number,
	
-----mencari total periode analisa kecelakaan dari awal kecelakaan sampai data terakhir masuk-----
---------------dan mengambil sisa dari pembagian untuk menentukan hari----------------------------
	
		DATE_PART('day', (date_trunc('day',a.last_date)-date_trunc('day',crash_cleansing.Waktu_Kejadian_Perkara)))::float - 		
		(7 * trunc (DATE_PART('day', (date_trunc('day',a.last_date)-date_trunc('day',crash_cleansing.Waktu_Kejadian_Perkara)))/7)) d_number  
		
FROM a, crash_cleansing
GROUP BY crash_cleansing.consecutive_number, a.last_date, crash_cleansing.Waktu_Kejadian_Perkara
ORDER BY d_number
) b, x
GROUP BY nomor_hari_kecelakaan, hari_kecelakaan, x.total_kecelakaan
ORDER BY 1;



WITH cta AS
(
SELECT	*,
------------------------------------Konversi Waktu Time Zone-------------------------------------------
		CASE
		WHEN state_name IN ('Alabama', 'Arkansas', 'Illinois', 'Iowa', 'Kansas','Louisiana',
						   'Minnesota', 'Mississippi', 'Missouri', 'Nebraska', 'North Dakota',
							'South Dakota', 'Oklahoma','Tennessee','Texas','Wisconsin') 
						THEN timestamp_of_crash at time zone 'CST' --(min 6 jam)
		WHEN state_name IN ('Connecticut', 'Delaware', 'District of Columbia', 'Florida','Georgia',
						   'Indiana', 'Kentucky', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
						   'New Hampshire', 'New Jersey', 'New York', 'North Carolina', 'Ohio','Pennsylvania',
						   'Rhode Island', 'South Carolina', 'Vermont', 'Virginia', 'West Virginia') 
						THEN timestamp_of_crash at time zone 'CDT' --(min 5 jam)
		WHEN state_name IN ('Alaska') THEN timestamp_of_crash at time zone 'AKST' --(min 9 jam)
		WHEN state_name IN ('Hawaii') THEN timestamp_of_crash at time zone 'HST' --(min 10 jam)
		WHEN state_name IN ('Arizona', 'Colorado', 'Idaho', 'Montana', 'New Mexico', 'Utah', 'Wyoming') 
						THEN timestamp_of_crash at time zone 'MST' --(min 7 jam)
		ELSE timestamp_of_crash at time zone 'PST' --(min 8 jam)
		END Waktu_Kejadian_Perkara
FROM crash
-----------------------------------------Cleansing---------------------------------------------------
WHERE 	milepoint NOT IN (99999, 99998)
AND		land_use_name NOT IN ('Unknown', 'Not Reported')
AND		functional_system_name NOT IN ('Unknown', 'Not Reported')
AND		type_of_intersection_name NOT IN ('Not Reported')
AND		atmospheric_conditions_1_name NOT IN ('Not Reported')
AND		city_name NOT IN ('Not Reported', 'Unknown', 'Other')
)

SELECT 	jam,
		COUNT(1)/AVG(total_hari)::numeric rata_rata_kecelakaan
FROM
(
SELECT 	*,
		EXTRACT ('hour' FROM Waktu_Kejadian_Perkara) jam,
		EXTRACT ('day' FROM (max(Waktu_Kejadian_Perkara) over()-min(Waktu_Kejadian_Perkara)over())) total_hari
FROM cta
) x
GROUP BY 1
ORDER BY 1



/*Persentase kecelakaan di daerah Perkotaan dan Pedesaan*/
select  land_use_name, 
        sum(number_of_fatalities) as jumlah_korban_meninggal,
		sum(number_of_drunk_drivers) as jumlah_pengemudi_yg_mabuk
		--count(land_use_name) as jumlah_angka_kecelakaan_di_suatu_daerah       
from    crash

where   light_condition_name not in ('Not Reported','Reported as Unknown')
and     atmospheric_conditions_1_name not in ('Not Reported','Reported as Unknown')
and     milepoint not in (99999, 99998)
and     functional_system_name not in ('Unknown', 'Not Reported')
--and     land_use_name not in ('Unknown', 'Not Reported') 
group by land_use_name  
order by jumlah_pengemudi_yg_mabuk desc




/*10 Negara Bagian dengan angka kecelakaan tertinggi */
SELECT state_name, COUNT(consecutive_number) angka_kecelakaan 
FROM crash
WHERE 	city_name NOT IN ('NOT APPLICABLE','Not Reported','Unknown') 
		AND
		land_use_name NOT IN ('Not Reported','Unknown') 
		AND
		functional_system_name NOT IN ('Not Reported','Unknown') 
		AND
		milepoint NOT IN (99999,99998) 
		AND
		type_of_intersection_name NOT IN ('Reported as Unknown','Not Reported')
GROUP BY state_name
ORDER BY 2 DESC 
LIMIT 10;

/*10 Kota dengan angka kecelakaan tertinggi berdasarkan 
10 Negara Bagian dengan angka kecelakaan tertinggi*/
SELECT
	city_name,
	state_name, 		 	
	COUNT(consecutive_number) angka_kecelakaan
FROM crash
WHERE
	state_name IN 
	(SELECT state_name FROM crash
	 WHERE 	city_name NOT IN ('NOT APPLICABLE','Not Reported','Unknown') 
			AND
			land_use_name NOT IN ('Not Reported','Unknown') 
			AND
			functional_system_name NOT IN ('Not Reported','Unknown') 
			AND
			milepoint NOT IN (99999,99998) 
			AND
			type_of_intersection_name NOT IN ('Reported as Unknown','Not Reported')
	GROUP BY state_name
	ORDER BY COUNT(consecutive_number) DESC LIMIT 10)
	AND
	city_name NOT IN ('NOT APPLICABLE',
					  'Not Reported',
					  'Unknown') 
	AND
	land_use_name NOT IN ('Not Reported',
						  'Unknown') 
	AND
	functional_system_name NOT IN ('Not Reported',
								   'Unknown') 
	AND
	milepoint NOT IN (99999,99998) 
	AND
	type_of_intersection_name NOT IN ('Reported as Unknown',
									  'Not Reported')
GROUP BY 	state_name, 
			city_name 			
ORDER BY 3 DESC
LIMIT 10;




/*kondisi yang memperbesar risiko kecelakaan top 3 */

-- select * from crash

select distinct light_condition_name, 
       atmospheric_conditions_1_name, 
	   sum(number_of_fatalities) as Jumlah_korban_meninggal,
	   sum(number_of_drunk_drivers) as jumlah_pengemudi_yg_mabuk

from   crash 
where  light_condition_name in ('Daylight','Dark - Lighted', 'Dark - Not Lighted')
and    atmospheric_conditions_1_name in ('Clear','Cloudy', 'Rain') 
and    milepoint not in (99999, 99998)
and    functional_system_name not in ('Unknown', 'Not Reported')
and    land_use_name not in ('Unknown', 'Not Reported')
group by light_condition_name , atmospheric_conditions_1_name
order by Jumlah_korban_meninggal desc 

-----------------------------------------------------------------------------------
/*kondisi 'Clear' dan 'Daylight' top 5*/

select distinct manner_of_collision_name,
	   sum(number_of_fatalities) as Jumlah_korban_meninggal,
	   sum(number_of_drunk_drivers) as jumlah_pengemudi_yg_mabuk

from crash 
where  manner_of_collision_name in ('The First Harmful Event was Not a Collision with a Motor Vehicle in Transport',
								    'Angle', 'Front-to-Front','Front-to-Rear','Sideswipe - Same Direction')
and    light_condition_name in ('Daylight')
and    atmospheric_conditions_1_name in ('Clear')
and    milepoint not in (99999, 99998)
and    functional_system_name not in ('Unknown', 'Not Reported')
and    land_use_name not in ('Unknown', 'Not Reported')
group by manner_of_collision_name
order by Jumlah_korban_meninggal desc


-----------------------------------------------------------------------------------
/*kondisi 'Clear' dan 'Dark - Lighted' top 5*/

select distinct manner_of_collision_name,
	   sum(number_of_fatalities) as Jumlah_korban_meninggal,
	   sum(number_of_drunk_drivers) as Jumlah_pengemudi_yg_mabuk

from crash 
where  manner_of_collision_name in ('The First Harmful Event was Not a Collision with a Motor Vehicle in Transport',
								    'Angle', 'Front-to-Front','Front-to-Rear','Sideswipe - Same Direction')
and    light_condition_name in ('Dark - Lighted')
and    atmospheric_conditions_1_name in ('Clear')
and    milepoint not in (99999, 99998)
and    functional_system_name not in ('Unknown', 'Not Reported')
and    land_use_name not in ('Unknown', 'Not Reported')
group by manner_of_collision_name
order by Jumlah_korban_meninggal desc

-----------------------------------------------------------------------------------
/*kondisi 'Clear' dan 'Dark - Not Lighted' top 5*/

select distinct manner_of_collision_name,
	   sum(number_of_fatalities) as Jumlah_korban_meninggal,
	   sum(number_of_drunk_drivers) as Jumlah_pengemudi_yg_mabuk

from crash 
where  manner_of_collision_name in ('The First Harmful Event was Not a Collision with a Motor Vehicle in Transport',
								    'Angle', 'Front-to-Front','Front-to-Rear','Sideswipe - Same Direction')
and    light_condition_name in ('Dark - Not Lighted')
and    atmospheric_conditions_1_name in ('Clear')
and    milepoint not in (99999, 99998)
and    functional_system_name not in ('Unknown', 'Not Reported')
and    land_use_name not in ('Unknown', 'Not Reported')
group by manner_of_collision_name
order by Jumlah_korban_meninggal desc

-----------------------------------------------------------------------------------
/*daylight and all atmospheric*/

select distinct light_condition_name, 
       atmospheric_conditions_1_name, 
	   sum(number_of_fatalities) as Jumlah_korban_meninggal,
	   sum(number_of_drunk_drivers) as jumlah_pengemudi_yg_mabuk

from   crash 
where  light_condition_name in ('Daylight')
and    atmospheric_conditions_1_name not in ('Not Reported','Reported as Unknown')
and    milepoint not in (99999, 99998)
and    functional_system_name not in ('Unknown', 'Not Reported')
and    land_use_name not in ('Unknown', 'Not Reported')
group by light_condition_name , atmospheric_conditions_1_name
order by Jumlah_korban_meninggal desc

select distinct functional_system_name from crash

-----------------------------------------------------------------------------------
/*jmlh korban meninggal, jumlah pengemudi yg mabuk, daylight, type of intersection, manner of collision name */
select distinct type_of_intersection_name, 
       manner_of_collision_name, 
	   atmospheric_conditions_1_name,
	   sum(number_of_fatalities) as Jumlah_korban_meninggal,
	   sum(number_of_drunk_drivers) as jumlah_pengemudi_yg_mabuk

from   crash 
where  light_condition_name in ('Daylight')
and    atmospheric_conditions_1_name not in ('Not Reported','Reported as Unknown')
and    milepoint not in (99999, 99998)
and    functional_system_name not in ('Unknown', 'Not Reported')
and    land_use_name not in ('Unknown', 'Not Reported')
group by type_of_intersection_name , manner_of_collision_name, atmospheric_conditions_1_name
order by Jumlah_korban_meninggal desc limit 5



------Persentase pengemudi mabuk
select
case
when number_of_drunk_drivers = 0 then 'Tidak Mabuk'
when number_of_drunk_drivers = 1 then 'Mabuk'
else 'error'
end kondisi_pengemudi,
count(*)*100/cast((select count(*) from crash)as float) percentage
from crash
group by number_of_drunk_drivers;


-----Milepoint kecelakaan

---- Berdasarkan kejadian kecelakaan
select milepoint, manner_of_collision_name, count(*) from crash
where milepoint in (
	select milepoint from crash
	group by milepoint
	order by count(*) desc
	limit 5
)group by milepoint, manner_of_collision_name;


--- Berdasarkan jumlah yang meninggal
select milepoint, manner_of_collision_name, sum(number_of_fatalities) from crash
where milepoint in (
	select milepoint from crash
	group by milepoint
	order by count(*) desc
	limit 5
)group by milepoint, manner_of_collision_name;


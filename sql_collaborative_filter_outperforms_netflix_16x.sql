-- Batch-based object-to-object collaborative filter.
-- Outperformed a slope-one model by 16x in replacing randomly-removed items from user's libraries.

-- CONCLUSION: RMSE may not be the ideal metric for evaluating performance of recommendation engines.


use TagDev;
 
declare @CompanyID int; 
declare @BeginningOfTime nvarchar(12); set @BeginningOfTime = '1/1/1960';
declare @MaxRowsToProcess int;  -- For now this is implemented as SELECT TOP N for performance purposes
declare @UseRating bit;
declare @MinRatingToUse tinyint;
 
-- for batch logging
declare @Agent varchar(50); set @Agent = 'Selloscope Conjugator v0.50';
declare @TimeStarted datetime; set @TimeStarted = (select getdate());
declare @TimeFinished datetime;
declare @BatchID int; 
declare @NumRecordsInBatch int;
declare @NumDedupedRecordsInBatch int;
declare @NumPairsInserted int;
declare @NumRecordsRemainingToBatch int;
declare @InsertNewPairsSuccess int; 
 
-- Find the next CompanyID to process, round-robin. 
set @CompanyID = (
                select  min(CompanyID)
                from    dbo.CompanySettings
                where   LastProcessingDate = 
                        (select min(LastProcessingDate) from dbo.CompanySettings where EnableProcessing='TRUE' )
                and     EnableProcessing='True'    
                );
                 
-- Set processing parameters 
-- set @MaxRowsToProcess = 10000;
set @UseRating = (select isnull(UseRating,'False') from dbo.CompanySettings where CompanyID = @CompanyID);
set @MinRatingToUse = (select isnull(MinRatingToUse,-10000) from dbo.CompanySettings where CompanyID = @CompanyID);
 
 
/*
declare @AgingDay int;
set @AgingDay = ((select datediff(dd, @BeginningOfTime, cast(cast(getdate() AS varchar(12)) as datetime)))-(select AgingFactor * 30.41667 from dbo.CompanySettings where CompanyID = @CompanyID));
*/
 
 
--Grab some records to process
--This is the "sample"
declare @CompanyID int; set @CompanyID = 20;
declare @BeginningOfTime nvarchar(12); set @BeginningOfTime = '1/1/1960';
SELECT TOP 500 
        RowID,
        InternalProductID,
        SesserID,
        IsUser,
        datediff(dd,@BeginningOfTime,TransactionDate) AS Day,
        Rating,
        --Category,
        --Price,
        TransactionDate,
        CompanyID
into    #InPairing
FROM    dbo.ProcessedData
where   ProcessedDataBatchID is null
and     CompanyID = @CompanyID
;
 
create index idx_CompanyID_SesserID_InternalProductID on #InPairing(CompanyID, SesserID, IsUser, InternalProductID);
create index idx_RowID on #InPairing(RowID);
 
set @NumRecordsInBatch = (select count(*) from #InPairing);
 
 
--drop table #InPairing
--select * from #InPairing
--update dbo.ProcessedData set ProcessedDataBatchID = null
 
 
 
 
--Identify records from this set to keep
select  a.CompanyID,
        a.SesserID,
        a.IsUser,
        a.InternalProductID,
        a.Day,
        min(RowID) AS RowID
into    #InPairingKeep
from    #InPairing a  --Here we join the Sample back to a set indicating which records are not duplicates
join    (
        --From the Sample, identify the characteristics of the highest rating in the case of duplicates
        select  CompanyID,
                SesserID,
                IsUser,
                InternalProductID,
                Day,
                max(Rating) AS Rating
        from    #InPairing
        group by    CompanyID,
                SesserID,
                IsUser,
                Day,
                InternalProductID
        -- having   count(*) > 1
        ) b on a.CompanyID = b.CompanyID and a.SesserID = b.SesserID and a.IsUser = b.IsUser and a.InternalProductID = b.InternalProductID and a.Day = b.Day
where a.Rating = b.Rating
group by    a.CompanyID,
            a.SesserID,
            a.IsUser,
            a.Day,
            a.InternalProductID
;
create index idx_CompanyID_SesserID_InternalProductID on #InPairingKeep(CompanyID, SesserID, IsUser, InternalProductID);
create index idx_RowID on #InPairingKeep(RowID);
--select * from #InPairingKeep
--drop table #InPairingKeep
 
 
--Identify records from this set to delete
select  a.SesserID,
        a.InternalProductID,
        a.RowID
into    #InPairingDelete
from    #InPairing a
left join   #InPairingKeep b on a.RowID = b.RowID
where   b.RowID is null
;
create index idx_RowID on #InPairingDelete(RowID);
--drop table #InPairingDelete
--select * from #InPairingDelete
 
 
--Delete all of these new crappy records from ProcessedData
delete  pd
from    dbo.ProcessedData pd
join    #InPairingDelete del on pd.RowID = del.RowID
;
 
--Now Delete all copies of these same new crappy records from the batch
delete  pd
from    #InPairing pd
join    #InPairingDelete del on pd.RowID = del.RowID
;
 
 
 
--OK, now we have a clean, de-duped batch set, with the possibility of a single duplicate in ProcessedData with a BatchID not null
--Need to break any ties where we have a duplicate in the batch set and one in ProcessedData
 
--first clean #InPairing...
delete  k
from    #InPairing k
join    dbo.ProcessedData pd on k.CompanyID = pd.CompanyID and k.SesserID = pd.SesserID and k.IsUser = pd.IsUser 
and     k.InternalProductID = pd.InternalProductID and k.Day = datediff(dd,'1/1/1960',pd.TransactionDate)
where   pd.ProcessedDataBatchID >= 0 
and     pd.ProcessedDataBatchID is not null  -- so, this avoids NULLS and -1's, which indicate parked data
and     k.Rating <= pd.Rating  -- removes any SAMPLED records with ratings less than or equal to a rating that already exists
;
 
--select * from #InPairing
 
--next try cleaning ProcessedData
delete  pd
from    dbo.ProcessedData pd
join    #InPairing k on k.CompanyID = pd.CompanyID and k.SesserID = pd.SesserID and k.IsUser = pd.IsUser 
and     k.InternalProductID = pd.InternalProductID and k.TransactionDate = datediff(dd,'1/1/1960',pd.TransactionDate)
where   pd.ProcessedDataBatchID >= 0
and     pd.ProcessedDataBatchID is not null
and     pd.Rating < k.Rating
;
 
--update dbo.ProcessedData set ProcessedDataBatchID = null where ProcessedDataBatchID = 0
--select * from dbo.ProcessedData where ProcessedDataBatchID =0
 
-- Mark the remaining records left in ProcessedData so we can join correctly
update dbo.ProcessedData
set     ProcessedDataBatchID = 0
from    dbo.ProcessedData pd
join    #InPairing k on pd.RowID = k.RowID
;
 
set @NumDedupedRecordsInBatch = (select count(*) from #InPairing);
 
--select * from dbo.ProcessedData where ProcessedDataBatchID =0is not null
 
 
 
--OK, now ProcessedData and the Batch set are entire clean. Join at will.
select  new.CompanyID,
        new.SesserID,
        new.IsUser,
        new.InternalProductID AS InternalProductID1,
        old.InternalProductID AS InternalProductID2,
        new.Day,
        --new.Rating AS NewRating,
        --old.Rating AS OldRating,
        --case when new.InternalProductID < old.InternalProductID then new.InternalProductID else old.InternalProductID end AS InternalProductID1,
        --case when new.InternalProductID < old.InternalProductID then old.InternalProductID else new.InternalProductID end AS InternalProductID2,
        cast(1 AS int) AS Freq,
        --cast(case 
        --  when new.InternalProductID < old.InternalProductID then cast(ISNULL(new.Rating,0) - ISNULL(old.Rating,0) AS int) 
        --      else cast(ISNULL(old.Rating,0) - ISNULL(new.Rating,0) AS int) 
        --end AS int) AS Diff
        cast(ISNULL(new.Rating,0) - ISNULL(old.Rating,0) AS int) AS Diff
into    #Paired
from    #InPairing new
join    dbo.ProcessedData old
on      new.CompanyID = old.CompanyID
and     new.SesserID = old.SesserID
and     new.IsUser = old.IsUser
and     new.Day !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!hshitshit
where   old.ProcessedDataBatchID is not null
and     new.InternalProductID < old.InternalProductID --Yes, this is still necessary despite the de-duping above. Try it and see for yourself if you have to.
--and       new.Rating >= @MinRatingToUse
--and       old.Rating >= @MinRatingToUse
 
--drop table #Paired
--drop table #Paired2
--create clustered index idx_idx on #Paired(InternalProductID1, InternalProductID2,Day);
 
set @NumPairsInserted = (select count(*) from #Paired);
 
--select * from #Paired2
 
 
--Success / Error logging
set @BatchID = (SELECT IDENT_CURRENT('dbo.ProcessedDataBatches'));
 
 
 
select  InternalProductID1,
        InternalProductID2,
        Day,
        sum(Freq) AS Freq,
        sum(Diff) AS Diff
into    #PairedDOWN
from    #Paired 
group by  InternalProductID1,
        InternalProductID2,
        Day
;
create clustered index idx_idx on #PairedDOWN(InternalProductID1, InternalProductID2,Day);
 
 
begin try
    begin transaction
     
        merge into dbo.ProcessedPairs as Target
        using #PairedDOWN as Source
        on Target.InternalProductID1 = Source.InternalProductID1
        and Target.InternalProductID2 = Source.InternalProductID2
        and Target.Day = Source.Day
        when matched then 
            update
                set     Target.Freq += Source.Freq,
                        Target.Diff += Source.Diff
        when not matched then
            insert (InternalProductID1,InternalProductID2,Day,Freq,Diff) values (Source.InternalProductID1, Source.InternalProductID2, Source.Day, Source.Freq, Source.Diff)
        ;
    commit transaction;
     
    update dbo.ProcessedData
    set     ProcessedDataBatchID = @BatchID
    from    dbo.ProcessedData pd
    join    #InPairing k on pd.RowID = k.RowID
;
     
    set @InsertNewPairsSuccess = 1;
 
end try
 
begin catch
    set @InsertNewPairsSuccess = 0;
     
    --Park the ProcessedData records that botched
    update dbo.ProcessedData
    set     ProcessedDataBatchID = -1
    where   ProcessedDataBatchID = 0
;
end catch;
 
 
--Update the batch record
set @NumRecordsRemainingToBatch = (select count(*) from dbo.ProcessedData where ProcessedDataBatchID is null);
set @TimeFinished = (select getdate());
 
 
 
-- Success / Error logging
if @InsertNewPairsSuccess = 1
begin
    insert into dbo.ProcessedDataBatches
    values (@CompanyID, @TimeStarted, @TimeFinished, @Agent, @NumRecordsInBatch, @NumDedupedRecordsInBatch, @NumPairsInserted, @NumRecordsRemainingToBatch, '');
end
else begin
    insert into dbo.ProcessedDataBatches
    values (@CompanyID, @TimeStarted, @TimeFinished, @Agent, @NumRecordsInBatch, @NumDedupedRecordsInBatch, 0, @NumRecordsRemainingToBatch, 'BATCH FAIL');
end
;
 
 
 
 
-- Reset the next company to be processed
update dbo.CompanySettings
set LastProcessingDate = (select getdate())
where   CompanyID = @CompanyID
;
 
 
drop table #InPairing;
drop table #InPairingKeep;
drop table #InPairingDelete;
drop table #Paired;
drop table #PairedDown;
 
 
--select * from dbo.ProcessedDataBatches order by ProcessedDataBatchID desc; --85
--select * from dbo.CompanySettings; --10/17
--select count(*) from dbo.ProcessedPairs; --4950
--select count(*) from dbo.ProcessedData where ProcessedDataBatchID is null;  --266532
 
-- ~fin
 
/*
select Freq,
        count(*) AS Cnt
from    dbo.ProcessedPairs
group by    Freq
order by    count(*)
*/

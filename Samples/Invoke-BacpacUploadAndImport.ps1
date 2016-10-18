#---------------------------------------------------------------------------------
# The sample scripts are not supported under any Microsoft standard support
# program or service. The sample scripts are provided AS IS without warranty
# of any kind. Microsoft further disclaims all implied warranties including,
# without limitation, any implied warranties of merchantability or of fitness for
# a particular purpose. The entire risk arising out of the use or performance of
# the sample scripts and documentation remains with you. In no event shall
# Microsoft, its authors, or anyone else involved in the creation, production, or
# delivery of the scripts be liable for any damages whatsoever (including,
# without limitation, damages for loss of business profits, business interruption,
# loss of business information, or other pecuniary loss) arising out of the use
# of or inability to use the sample scripts or documentation, even if Microsoft
# has been advised of the possibility of such damages.
#---------------------------------------------------------------------------------

<#
 	.SYNOPSIS
       This script migrates a set of bacpacs to a set of corresponding Azure SQL Database databases.

    .DESCRIPTION
       Bacpacs are uploaded using AzCopy from a local directory to an Azure blob storage container.
       Then, for each bacpac, an import operation is started asynchronously.
       The script runs until all import operations complete, and outputs the status of all running
       and failed import operations. Before completion, the script validates that for each source bacpac,
       a database with matching name exists, and throws an exception otherwise.

    .NOTES
       Prerequisites:
         Azure PowerShell installed
         AzCopy installed

       Assumptions:
         Sufficient permissions on subscription, storage account, and destination logical server are granted
         Server DTU quota will not be exceeded by importing all bacpacs
         All bacpacs found in the container are to be imported
         Desired database name matches bacpac base name
         Databases with the same name as bacpac base name do not already exist on target logical server
         All databases imported in a single script execution will have the same SLO
         A v2 storage account is used

       Version: 1.0.0

	.EXAMPLE
        .\Invoke-BacpacUploadAndImport.ps1 -SubscriptionId "<SubscriptionId>" `
                                           -BacpacSourceDirectory "<BacpacSourceDirectory>" `
                                           -SqlDbServerName "<SqlDbServerName>" `
                                           -SqlDbServerAdminAccountName "<SqlDbServerAdminAccountName>" `
                                           -DbEdition "<DbEdition>" `
                                           -DbServiceLevel "<DbServiceLevel>" `
                                           -BacpacStorageAccountName "<StorageAccountName>" `
                                           -BacpacContainerName "<BacpacContainerName>" `
                                           -UploadBacpacs $true `
                                           -StorageResourceGroup "<StorageResourceGroup>" `
                                           -SqlResourceGroup "<SqlResourceGroup>"
#>

param(
     [Parameter(Mandatory=$true)]
     [string] $StorageResourceGroup,

     [Parameter(Mandatory=$true)]
     [string] $SqlResourceGroup,

     [parameter(Mandatory=$true)]
     [string] $SqlDbServerName,

     [parameter(Mandatory=$true)]
     [string] $BacpacStorageAccountName,

     [parameter(Mandatory=$true)]
     [string] $BacpacSourceDirectory,

     [parameter(Mandatory=$true)]
     [string] $SqlDbServerAdminAccountName,

     [parameter(Mandatory=$true)]
     [string] $BacpacContainerName = 'bacpacs',

     [parameter(Mandatory=$true)]
     [guid] $SubscriptionId,

     [parameter(Mandatory=$true)]
     [string] $DbEdition,

     [parameter(Mandatory=$true)]
     [string] $DbServiceLevel,

     [string] $AZCopyPath = 'C:\Program Files (x86)\Microsoft SDKs\Azure\AzCopy\AzCopy.exe',

     [bool] $UploadBacpacs = $true # Set to $false if bacpacs have already been uploaded
     )

try {

$StartTime = Get-Date

Login-AzureRmAccount | Out-Null

Select-AzureRmSubscription -SubscriptionId $SubscriptionId | Out-Null

$SqlCred = $host.ui.PromptForCredential("Enter credentials", "Enter the login name and password to authenticate to target Azure SQL Database server", $SqlDbServerAdminAccountName, "")
$SqlCred.Password.MakeReadOnly()

$StorageAccountKey = (Get-AzureRmStorageAccountKey -ResourceGroupName $StorageResourceGroup -Name $BacpacStorageAccountName).Value[0]

# Conditionally upload all bacpacs in specified directory
if ($UploadBacpacs) {
    Write-Output "Starting AzCopy to upload all bacpac files in '$BacpacSourceDirectory' to container '$BacpacContainerName' in storage account '$BacpacStorageAccountName'"

    $Arg1 = '/Source:"' + $BacpacSourceDirectory + '"'
    $Arg2 = '/Dest:"https://' + $BacpacStorageAccountName + '.blob.core.windows.net/' + $BacpacContainerName + '"'
    $Arg3 = '/DestKey:' + $StorageAccountKey
    $Arg4 = '/Pattern:*.bacpac'
    $Arg5 = '/Y' # Skip all AzCopy prompts. This will overwrite any existing blobs.
    & $AzCopyPath $Arg1 $Arg2 $Arg3 $Arg4 $Arg5
}

# Enumerate bacpac blobs in the container
$StorageContext = New-AzureStorageContext -StorageAccountName $BacpacStorageAccountName -StorageAccountKey $StorageAccountKey

$BacpacBlobs = Get-AzureStorageContainer -Context $StorageContext -Name $BacpacContainerName `
| Get-AzureStorageBlob `
| Where-Object Name -ilike "*.bacpac"

# Declare a collection of import operation objects, to be used for status reporting
$ImportOperations = @()

# Start an import operation for each bacpac, and add the import operation object to collection
$BacpacBlobs | ForEach-Object {

    $BacpacBlobName = $_.Name
    $DbName = $BacpacBlobName.Split('\.')[-2]

    Write-Output "Starting asynchronous import of bacpac '$BacpacBlobName' into database '$DbName'"

    $ImportOperation = $null

    $StorageUri = "https://$BacpacStorageAccountName.blob.core.windows.net/$BacpacContainerName/$BacpacBlobName"

    $ImportOperation = New-AzureRmSqlDatabaseImport -ResourceGroupName $SqlResourceGroup `
                                                    -ServerName $SqlDbServerName `
                                                    -DatabaseName $DbName `
                                                    -Edition $DbEdition `
                                                    -ServiceObjectiveName $DbServiceLevel `
                                                    -DatabaseMaxSizeBytes 0 `
                                                    -AdministratorLogin $SqlCred.UserName `
                                                    -AdministratorLoginPassword $SqlCred.Password `
                                                    -StorageKeyType StorageAccessKey `
                                                    -StorageKey $StorageAccountKey `
                                                    -StorageUri $StorageUri `
                                                    -ErrorAction Continue

    if (!$ImportOperation) {
        Write-Warning "Failed to start bacpac import operation for $BacpacBlobName."
    }
    else {
        Write-Output ("Started import operation. Operation status link is {0}." -f $ImportOperation.OperationStatusLink)
    }

    $ImportOperations += $ImportOperation
}

$TotalCount = $ImportOperations.count

# Output periodic progress reports for as long as there are in progress import operations
do  {
    # Get a collection of status objects for each import operation
    $ImportOperationsStatus = $ImportOperations | ForEach-Object {
        Get-AzureRmSqlDatabaseImportExportStatus $_.OperationStatusLink -ErrorAction Continue
        # For some failed operations, this cmdlet re-throws the exception(s) that caused the failure, instead of returning a status object.
        # Exclude such operations from progress report.
    }

    # Filter out succeeded operations
    $RunningImportOperationsStatus = $ImportOperationsStatus | Where-Object -FilterScript {$_.Status -ne "Succeeded"}

    $dt = Get-Date
    $RunningCount = $RunningImportOperationsStatus.count
    Write-Output "Operation status as of $dt : $RunningCount in-progress/failed out of $TotalCount queued. Detailed status of in-progress/failed operations follows:"

    $RunningImportOperationsStatus | ForEach-Object {
        $_ | Select-Object OperationStatusLink,ErrorMessage,QueuedTime,LastModifiedTime,Status,StatusMessage | fl *
    }

    Start-Sleep -Seconds 15

} while (($ImportOperationsStatus | Where-Object -FilterScript {$_.Status -eq "InProgress"}).count -gt 0)

# Throw an exception if there are any failed import operations
$FailedOpCount = ($ImportOperationsStatus | Where-Object -FilterScript {$_.Status -eq "Failed"}).count

if ($FailedOpCount -gt 0) {
    Write-Warning "Some import operations have failed."

    $ImportOperationsStatus | Where-Object -FilterScript {$_.Status -eq "Failed"} | ForEach-Object {
        $_ | Select-Object OperationStatusLink,ErrorMessage,QueuedTime,LastModifiedTime,Status,StatusMessage | fl *
    }

    throw "$FailedOpCount import operations have failed"
}

# Throw an exception if there are source bacpacs that do not have a matching database
if ($BacpacBlobs) {
    $MissedBacpacs = $BacpacBlobs | Where-Object {
        !(Get-AzureRmSqlDatabase -ResourceGroupName $SqlResourceGroup -ServerName $SqlDbServerName -DatabaseName $_.Name.Split('\.')[-2] -ErrorAction SilentlyContinue)
    }

    if ($MissedBacpacs) {
        Write-Warning "Databases for following bacpacs are missing on destination Azure SQL Database server:"
        $MissedBacpacs | Select-Object Name | Format-Table -Wrap -AutoSize

        $MissedBacpacCount = $MissedBacpacs.count

        throw "$MissedBacpacCount databases are missing on destination Azure SQL Database server"
    }
}

}
catch {
    throw
}
finally {
    $elapsedTime = New-Timespan $StartTime $(Get-Date)
    Write-Output ("Completed in {0:hh} hours, {0:mm} minutes, {0:ss} seconds" -f $elapsedTime)
}

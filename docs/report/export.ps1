<#
.SYNOPSIS
    Finish the report in Word: update its fields (contents, lists of figures and tables, caption numbers), save the
    .docx and export the same document as a PDF with heading bookmarks.

        node docs\report\build.mjs
        .\docs\report\export.ps1

.DESCRIPTION
    Uses Microsoft Word through COM, invisibly, and closes it afterwards. The PDF is Word's own rendering of the
    .docx, so both files show the same report.
#>
[CmdletBinding()]
param([string]$Docx = (Join-Path $PSScriptRoot '..\..\submission\VeloGraph-Report.docx'))
$ErrorActionPreference = 'Stop'
$docxPath = (Resolve-Path $Docx).Path
$pdfPath = [IO.Path]::ChangeExtension($docxPath, '.pdf')

$word = New-Object -ComObject Word.Application
$word.Visible = $false
$word.DisplayAlerts = 0
try {
    $doc = $word.Documents.Open($docxPath, $false, $false, $false)
    # Caption numbers (SEQ fields) first, then the tables built from them, then once more for page numbers.
    $null = $doc.Fields.Update()
    foreach ($toc in $doc.TablesOfContents) { $toc.Update() }
    foreach ($tof in $doc.TablesOfFigures) { $tof.Update() }
    $null = $doc.Fields.Update()
    foreach ($toc in $doc.TablesOfContents) { $toc.UpdatePageNumbers() }
    $doc.Save()
    # 17 = wdExportFormatPDF, 0 = print quality, 1 = bookmarks from headings.
    $doc.ExportAsFixedFormat($pdfPath, 17, $false, 0, 0, 0, 0, 0, $true, $true, 1, $true, $true, $false)
    $pages = $doc.ComputeStatistics(2)
    $doc.Close(0)
    Write-Host ("Saved {0} and {1} ({2} pages)" -f $docxPath, $pdfPath, $pages)
} finally {
    $word.Quit()
    [void][Runtime.InteropServices.Marshal]::ReleaseComObject($word)
}

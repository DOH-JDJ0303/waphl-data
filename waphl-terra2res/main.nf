#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

/*
=============================================================================================================================
    GET TIMESTAMP
=============================================================================================================================
*/
Date now = new Date()
long now_unix = now.getTime() / 1000
println "Files will be saved with timestamp: ${now_unix}"

/*
=============================================================================================================================
    PREPARE OUTPUTS
=============================================================================================================================
*/
// Log File
def log_name = "${now_unix}-waphl-terra2res.log"
def log_tmp = file(workflow.workDir).resolve(log_name)
def log_file = file(params.outdir).resolve("logs").resolve(log_name)
log_tmp.text = "Note: If this message is not updated it means the workflow failed."
log_tmp.moveTo(log_file)

// Meta File
def meta_tmp  = file(workflow.workDir).resolve("${now_unix}.csv")
def meta_file = file(params.outdir).resolve("meta").resolve("${now_unix}.csv")

workflow {

    /*
    =============================================================================================================================
        LOAD INPUT CHANNEL
    =============================================================================================================================
    */
    Channel
        .fromPath(file(params.input))
        .splitCsv(header: true)
        .set{ ch_input }

    /*
    =============================================================================================================================
        CHECK FOR ACTIVITY
    =============================================================================================================================
    */

    ch_input
        .filter{ ! it.run }
        .map{ it + [ cache: file(params.outdir).resolve("cache").resolve("terra").resolve(it.project).resolve(it.workspace) ] }
        .map{ [ it.project, it.workspace, it.cache.exists() ? it.cache.listFiles().toList() : null ] }
        .transpose()
        .filter{ project, workspace, cache -> cache }
        .map{ project, workspace, cache -> [ project, workspace, [ cache, cache.lastModified() ] ] }
        .groupTuple(by: [0,1])
        .map{ project, workspace, cache -> [ project, workspace, cache.max{ cache[1] }[0] ] }
        .splitCsv(header: true, sep: '\t', elem: 2)
        .map{ project, workspace, cache -> [ project: project, workspace: workspace ] + filter_summary(cache) }
        .filter{ it.retain }
        .set{ ch_cache }

    MONITOR (
        ch_input.filter{ ! it.run }.map{ [ it.project, it.workspace ] }
    )

    MONITOR
        .out
        .summary
        .splitCsv(header: true, sep: '\t', elem: 2)
        .map{ project, workspace, summary -> [ project: project, workspace: workspace ] + filter_summary(summary) }
        .filter{ it.retain }
        .map{ [ it.project, it.workspace, it.submissionId, it ] }
        .join( ch_cache.map{ [ it.project, it.workspace, it.submissionId, "old" ] } , remainder: true, by: [0,1,2] )
        .map{ project, workspace, submissionId, summary, status -> summary + [ status: status ] }
        .filter{ ! it.status }
        .map{ [ it.project, it.workspace, it.entity ] }
        .unique()
        .combine(ch_input.map{ [ it.project, it.workspace, it.workflow ] }, by: [0,1])
        .concat(ch_input.filter{ it.run }.map{ [ it.project, it.workspace, it.run, it.workflow ] })
        .set{ ch_tables }

    EXPORT_TABLE (
        ch_tables
    )

    // channel for saving the Terra Table - this is a run-level file (i.e., sample = null)
    EXPORT_TABLE
        .out
        .table
        .map{ workflow, run, table -> [ workflow, run, null, table ] }
        .set{ ch_tables }

    // get list of files in each Terra Table.
    EXPORT_TABLE
        .out
        .table
        .splitCsv(elem: 2, sep: '\t', header: true)
        .map{ workflow, run, table -> [ workflow, run, table["${run}_id"], table.values().toList() ] }
        .transpose()
        .filter{ workflow, run, sample, result -> result.contains("gs://") }
        .filter{ workflow, run, sample, f -> file(f).exists() }
        .concat(ch_tables)
        .map{ workflow, run, sample, f -> [ run: run, workflow: workflow, timestamp: (file(f).lastModified() / 1000).round(), sample: sample, fileorigin: file(f), filename: file(f).getName(), runname: run ] }
        .map{  it + [ filedest: file(params.outdir, checkIfExists: false ) / "data" / "id=${it.sample}" / "workflow=${it.workflow}" / "run=${it.runname}" / "file=${it.filename}" / "timestamp=${it.timestamp}" / it.filename ] }
        .map{ it + [ strorigin: pathToString(it.fileorigin), strdest: pathToString(it.filedest) ] }
        .unique()
        .filter{ it.filedest.exists() ? false : true }
        .set{ ch_files }
        
    /*
    =============================================================================================================================
        TRANSFER FILES
    =============================================================================================================================
    */

    THROTTLE (
        ch_files.collate(params.batchsize)
    )

    THROTTLE
        .out
        .files
        .flatten()
        .set{ ch_th_files }
    
    ch_th_files.subscribe{ file(it.fileorigin).copyTo(file(it.filedest)) }

    Channel
        .of("id,workflow,run,file,timestamp,origin,current")
        .concat(ch_th_files.map{ "${it.sample},${it.workflow},${it.runname},${it.filename},${it.timestamp},${it.strorigin},${it.strdest}" })
        .collect()
        .subscribe{ meta_tmp.text = it.join('\n')
                    if(count_lines(meta_tmp) > 1){meta_tmp.copyTo(meta_file)} }        
}

workflow.onComplete {

    def file_count = meta_tmp.exists() ? count_lines(meta_tmp) : 1
    meta_file  = file_count > 1 ? meta_file : "None"

    def msg = """\
        Pipeline execution summary
        ---------------------------
        Timestamp         : ${now_unix}
        Log file          : ${log_file}
        Metadata file     : ${meta_file}
        Files transferred : ${file_count - 1}
        Completed at      : ${workflow.complete}
        Duration          : ${workflow.duration}
        Success           : ${workflow.success}
        Exit status       : ${workflow.exitStatus}
        """
        .stripIndent()
    
    println msg
    
    log_file.text = msg

    if(params.email){sendMail(to: params.email, subject: "waphl-prod2res: ${now_unix}", body: msg)}
}

/*
=============================================================================================================================
    FUNCTIONS
=============================================================================================================================
*/
def filter_summary(LinkedHashMap row){\
    def entity        = row.submissionEntity.split(":")[0]
    def run_status    = row.status == "Done" ? true : false
    def sample_status = row.workflowStatuses.contains("Succeeded") ? true : false
    def set_status    = entity.contains("_set") ? false : true
    
    def result = [ entity: entity,
                   submissionId: row.submissionId, 
                   submissionDate: row.submissionDate, 
                   retain: run_status && sample_status && set_status ? true : false ]
    return result
}

// convert to string to path, retaining the schema
def pathToString(path){
    def schema = path.getScheme()
    def pathstring = path.toString()
    def result = path.getScheme() == "file" ? pathstring : "${schema}:/${pathstring}"
    return result
}

// count lines in a file
def count_lines(myFile){
    count = 0
    myFile.eachLine { str -> count++ }
    return count
}

/*
=============================================================================================================================
    MODULES
=============================================================================================================================
*/

process MONITOR {

    input:
    tuple val(project), val(workspace)

    output:
    tuple val(project), val(workspace), path('*.tsv'), emit: summary

    script:
    """
    fissfc monitor -p ${project} -w ${workspace} > ${project}-${workspace}-summary.tsv
    """
}

process EXPORT_TABLE {

    input:
    tuple val(project), val(workspace), val(table), val(workflow)

    output:
    tuple val(workflow), val(table), path('terra_table.tsv'), emit: table

    script:
    """
    export_large_tsv.py -p ${project} -w ${workspace} -e ${table} -f terra_table.tsv
    """
}

process THROTTLE {
    maxForks 1

    input:
    val it

    output:
    val it, emit: files

    script:
    """
    sleep 10
    """

}


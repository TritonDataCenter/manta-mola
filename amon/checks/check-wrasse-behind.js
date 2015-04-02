#!/usr/bin/env node
/* vim: set ft=javascript: */

/*
 * check-wrasse-behind: reads archive status on stdin and exits 0 iff wrasse is
 * okay.  We don't handle various errors gracefully because if anything goes
 * wrong, we want the command to exit with non-zero status anyway.
 */

/*
 * The default linger time is 24 hours.  If a job is 25 hours old, we consider
 * that an error.
 */
var maxlingertime = 25 * 60 * 60 * 1000;

/*
 * We run jobs every hour, so if nothing has been archived for two hours, that's
 * a problem.
 */
var maxidletime = 2 * 60 * 60 * 1000;

/*
 * Wrasses hould generally assign itself to jobs and archive them quickly so
 * there shouldn't be much of a backlog.
 */
var maxjobsassigned = 100;
var maxjobsunassigned = 30;

function main()
{
        var data = '';
        process.stdin.on('data', function (chunk) {
                data += chunk.toString('utf8');
        });
        process.stdin.on('end', function () {
                var parsed;

                parsed = JSON.parse(data);
                checkStatus(parsed);
        });
}

function checkStatus(st)
{
        var now, oldestjobtime, lastarchivetime;
        var nassigned, nunassigned;
        var nerrors = 0;

        now = Date.now();
        if (st.jobNextDelete !== null) {
                oldestjobtime = Date.parse(st.jobNextDelete.timeArchiveDone);
                if (now - oldestjobtime > maxlingertime) {
                        console.error('job has been lingering too long',
                            st.jobNextDelete);
                        nerrors++;
                }
        }

        if (st.jobLastArchived !== null) {
                lastarchivetime = Date.parse(
                    st.jobLastArchived.timeArchiveDone);
                if (now - lastarchivetime > maxidletime) {
                        console.error('job has been unarchived too long',
                            st.jobLastArchived);
                        nerrors++;
                }
        }

        nassigned = st.nJobsAssigned;
        if (nassigned > maxjobsassigned) {
                console.error('too many jobs assigned but not archived ' +
                    '(%s allowed, found %s)', maxjobsassigned, nassigned);
                nerrors++;
        }

        nunassigned = st.nJobsDone - st.nJobsArchived - st.nJobsAssigned;
        if (nunassigned > maxjobsunassigned) {
                console.error('too many jobs done but unassigned to a wrasse ' +
                    '(%s allowed, found %s)', maxjobsunassigned, nunassigned);
                nerrors++;
        }

        if (nerrors > 0)
                process.exit(1);
}

main();

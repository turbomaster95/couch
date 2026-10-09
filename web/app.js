function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function statusClass(status) {
    if (status === "success") return "status-success";
    if (status === "failed") return "status-failed";
    if (status === "running") return "status-running";
    return "status-queued";
}

async function getJson(url) {
    const response = await fetch(url);

    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);

    return response.json();
}

async function loadJobs() {
    const container = document.getElementById("jobs");

    if (!container) return;

    try {
        const jobs = await getJson("/api/v1/jobs");

        if (!jobs.length) {
            container.innerHTML = '<span class="dim">no jobs</span>';
            return;
        }

        container.innerHTML = jobs.map(job => `
            <div class="job">
                <span><a href="/job.html?id=${encodeURIComponent(job.id)}">#${escapeHtml(job.id)}</a></span>
                <span>${escapeHtml(job.repository)}</span>
                <span>${escapeHtml(job.ref)}</span>
                <span class="${statusClass(job.status)}">${escapeHtml(job.status)}</span>
                <span>${escapeHtml(job.runner ?? "-")}</span>
            </div>
        `).join("");
    } catch (error) {
        container.innerHTML = `<span class="error">${escapeHtml(error.message)}</span>`;
    }
}

function getJobId() {
    return new URLSearchParams(window.location.search).get("id");
}

async function loadJob() {
    const container = document.getElementById("job");

    if (!container) return;

    const id = getJobId();

    if (!id) {
        container.innerHTML = '<span class="error">missing job id</span>';
        return;
    }

    try {
        const job = await getJson(`/api/v1/jobs/${encodeURIComponent(id)}`);

        if (!document.getElementById("job-content")) {
            container.innerHTML = `
                <div id="job-content">
                    <div class="command">$ cat job/${escapeHtml(job.id)}</div>

                    <div class="meta">
                        <div class="meta-key">repository</div>
                        <div id="repository"></div>

                        <div class="meta-key">ref</div>
                        <div id="ref"></div>

                        <div class="meta-key">commit</div>
                        <div id="commit"></div>

                        <div class="meta-key">runner</div>
                        <div id="runner"></div>

                        <div class="meta-key">status</div>
                        <div id="status"></div>

                        <div class="meta-key">exit code</div>
                        <div id="exit-code"></div>

                        <div class="meta-key">signal</div>
                        <div id="signal"></div>
                    </div>

                    <div class="section">
                        <div class="section-title">$ log</div>
                        <pre id="log" class="log">loading...</pre>
                    </div>

                    <div class="section">
                        <div class="section-title">$ artifacts</div>
                        <div id="artifacts">loading...</div>
                    </div>
                </div>
            `;
        }

        document.getElementById("repository").textContent = job.repository;
        document.getElementById("ref").textContent = job.ref;
        document.getElementById("commit").textContent = job.commit;
        document.getElementById("runner").textContent = job.runner ?? "-";

        const status = document.getElementById("status");
        status.textContent = job.status;
        status.className = statusClass(job.status);

        document.getElementById("exit-code").textContent = job.exit_code;
        document.getElementById("signal").textContent = job.signal;

        await loadLog(id);
        await loadArtifacts(id);

        if (job.status === "queued" || job.status === "running") {
            setTimeout(loadJob, 1000);
        }
    } catch (error) {
        container.innerHTML = `<span class="error">${escapeHtml(error.message)}</span>`;
    }
}

async function loadLog(id) {
    const log = document.getElementById("log");

    if (!log) return;

    try {
        const response = await fetch(`/api/v1/jobs/${encodeURIComponent(id)}/log`);

        if (!response.ok) {
            log.textContent = "unable to load log";
            return;
        }

        log.textContent = await response.text();
        log.scrollTop = log.scrollHeight;
    } catch (error) {
        log.textContent = error.message;
    }
}

async function loadArtifacts(id) {
    const container = document.getElementById("artifacts");

    if (!container) return;

    try {
        const artifacts = await getJson(`/api/v1/jobs/${encodeURIComponent(id)}/artifacts`);

        if (!artifacts.length) {
            container.innerHTML = '<span class="dim">none</span>';
            return;
        }

        container.innerHTML = artifacts.map(artifact => `
            <div class="artifact">
                <a href="/api/v1/artifacts/${encodeURIComponent(artifact.id)}?job=${encodeURIComponent(id)}">${escapeHtml(artifact.name)}</a>
                <span class="dim">${formatSize(artifact.size)}</span>
            </div>
        `).join("");
    } catch (error) {
        container.innerHTML = `<span class="error">${escapeHtml(error.message)}</span>`;
    }
}

function formatSize(size) {
    if (size < 1024) return `${size} B`;

    if (size < 1024 * 1024) {
        return `${(size / 1024).toFixed(1)} KiB`;
    }

    if (size < 1024 * 1024 * 1024) {
        return `${(size / (1024 * 1024)).toFixed(1)} MiB`;
    }

    return `${(size / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
}

if (document.getElementById("jobs")) {
    loadJobs();
    setInterval(loadJobs, 2000);
}

if (document.getElementById("job")) {
    loadJob();
}

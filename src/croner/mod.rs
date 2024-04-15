use std::{collections::HashMap, fs::read_dir, sync::Arc};

use chrono::FixedOffset;
use failure::{format_err, Error};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task};
use tokio_cron_scheduler::{Job as RcronJob, JobScheduler};

use log::{debug, error, info};

use crate::job::Job;

#[derive(Debug, Serialize, Deserialize)]
pub struct Croner {
    pub jobs_folder: String,
    pub log_folder: String,
    pub log_level: String,
    pub watch: bool,
}

impl Croner {
    pub fn new(jobs_folder: String, log_folder: String, log_level: String, watch: bool) -> Self {
        Croner {
            jobs_folder,
            log_folder,
            log_level,
            watch,
        }
    }

    pub fn load_from_toml(path: &str) -> Self {
        let content = std::fs::read_to_string(path).unwrap();
        toml::from_str(&content).unwrap()
    }

    pub fn load(&self) -> Result<HashMap<String, Job>, Error> {
        info!("Load jobs folder {}", &self.jobs_folder);
        let mut jobs = HashMap::new();
        let paths = std::fs::read_dir(&self.jobs_folder)?;
        for path in paths {
            let path = path.unwrap().path();
            debug!("Reading path {:?}", path);
            debug!("Is file: {}", path.is_file());
            debug!(
                "Ends with toml: {}",
                path.extension().unwrap().to_str().unwrap() == "toml"
            );
            if path.is_file() & (path.extension().unwrap().to_str().unwrap() == "toml") {
                debug!("Reading file {:?}", path);
                let job = Job::load_from_toml(
                    &path
                        .to_str()
                        .ok_or(format_err!("Error reading file {:?}", &path.as_os_str()))?,
                )?;

                debug!("Job loaded {:?}", job.name);

                jobs.insert(path.to_str().unwrap().to_string().clone(), job);
            }
        }

        Ok(jobs)
    }

    pub async fn run(&self) -> Result<(), Error> {
        info!("Start running croner");
        let jobs = self.load()?;
        let scheduler = JobScheduler::new().await?;

        debug!("Jobs loaded {:?}", jobs.len());

        let job_mapping = Arc::new(Mutex::new(HashMap::new()));
        let job_path_mapping = Arc::new(Mutex::new(HashMap::new()));

        // Init jobs to scheduler
        info!("Init jobs to scheduler");
        for (path, job) in jobs.iter() {
            let job = job.clone();
            let job_name = job.name.clone();
            let cron_expr = job.cron_schedule.clone();
            let path = path.clone();

            debug!("Cron expr {:?}", cron_expr);

            let rcron_job = RcronJob::new_async_tz(
                cron_expr.as_str(),
                FixedOffset::east_opt(7 * 3600).unwrap(),
                move |uuid, _l| {
                    let mut job = job.clone();
                    Box::pin(async move {
                        let job_name = job.name.clone();

                        let current_timestamp = chrono::Local::now().timestamp();
                        let mut envs = HashMap::new();
                        envs.insert("RFLOWTS".to_string(), current_timestamp.to_string());

                        info!("Running job {} with uuid {}", job_name, uuid);

                        let result = job.run_with_envs(envs).await;
                        if let Err(e) = result {
                            error!(
                                "Error running job {} - {} with error: {}",
                                job_name, uuid, e
                            );
                        }
                    })
                },
            )?;

            debug!("Adding job {:?}", job_name);
            info!("Adding job {} with schedule '{}'", job_name, cron_expr.as_str());
                
            let guid = rcron_job.guid();

            scheduler.add(rcron_job).await?;

            job_mapping
                .lock()
                .await
                .insert(job_name.clone(), guid.clone());
            job_path_mapping
                .lock()
                .await
                .insert(path.clone(), guid.clone());
        }
        let scheduler = Arc::new(scheduler);

        let moved_scheduler = scheduler.clone();
        task::spawn(async move {
            moved_scheduler.start().await.unwrap();
        });

        let mut path_mapping = HashMap::new();

        let paths = read_dir(&self.jobs_folder)?;
        for path in paths {
            let path = path?.path();
            if !path.is_file() || path.extension().unwrap().to_str().unwrap() != "toml" {
                continue;
            }

            let last_modified = path.metadata()?.modified()?;
            path_mapping.insert(path.clone(), last_modified);
        }

        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            if !self.watch {
                continue;
            }

            let mut paths = read_dir(&self.jobs_folder)?;
            let mut remove_paths = Vec::new();

            // Check removed files
            for (path, _) in path_mapping.iter() {
                if paths.all(|p| &p.unwrap().path() != path) {
                    info!("Detecting removed file {:?}", path);
                    let source = path.to_str().unwrap().to_string();

                    let scheduler = scheduler.clone();
                    let job_path_mapping = job_path_mapping.clone();
                    let job_mapping = job_mapping.clone();

                    tokio::task::spawn(async move {
                        let uuid = match job_path_mapping.lock().await.get(&source) {
                            Some(uuid) => uuid.clone(),
                            None => {
                                error!("Error reading job uuid");
                                return;
                            }
                        };

                        match scheduler.remove(&uuid).await {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error removing job: {}", e);
                                return;
                            }
                        };

                        job_path_mapping.lock().await.remove(&source);

                        let mut job_name = "".to_string();
                        for (k, v) in job_mapping.lock().await.iter() {
                            if v == &uuid {
                                job_name = k.clone();
                                break;
                            }
                        }

                        job_mapping.lock().await.remove(&job_name);

                        info!("Removed job {}", job_name);
                    });

                    remove_paths.push(path.clone());
                }
            }

            for path in remove_paths {
                path_mapping.remove(&path);
                debug!("Removed path {:?}", path);
            }

            for path in paths {
                let path = path?.path();
                if !path.is_file() || path.extension().unwrap().to_str().unwrap() != "toml" {
                    continue;
                }

                let last_modified = path.metadata()?.modified()?;

                if path_mapping.contains_key(&path) {
                    // Check modified files
                    if path_mapping.get(&path).unwrap() != &last_modified {
                        info!("Detecting modified file {:?}", path);
                        let source = path.to_str().unwrap().to_string();

                        let modified_job = match Job::load_from_toml(&source) {
                            Ok(job) => job,
                            Err(e) => {
                                error!("Error reading job from file: {}", e);
                                continue;
                            }
                        };

                        let job_name = modified_job.name.clone();

                        let scheduler = scheduler.clone();
                        let job_mapping = job_mapping.clone();
                        let job_path_mapping = job_path_mapping.clone();

                        tokio::spawn(async move {
                            let old_uuid = match job_mapping.lock().await.get(&job_name) {
                                Some(uuid) => uuid.clone(),
                                None => {
                                    error!("Error reading job uuid");
                                    return;
                                }
                            };

                            match scheduler.remove(&old_uuid).await {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("Error removing job: {}", e);
                                    return;
                                }
                            };

                            job_mapping.lock().await.remove(&job_name);
                            job_path_mapping.lock().await.remove(&source);

                            match Self::add_job(
                                scheduler,
                                source.to_string(),
                                modified_job,
                                job_mapping,
                                job_path_mapping,
                            )
                            .await
                            {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("Error adding job: {}", e);
                                    return;
                                }
                            }

                            info!("Modified job {}", job_name);
                        });

                        path_mapping.insert(path.clone(), last_modified);
                    }
                } else {
                    // Check new files
                    info!("Detecting new file {:?}", path);
                    let source = path.to_str().unwrap().to_string();

                    let new_job = match Job::load_from_toml(&source) {
                        Ok(job) => job,
                        Err(e) => {
                            error!("Error reading job from file: {}", e);
                            continue;
                        }
                    };

                    let scheduler = scheduler.clone();
                    let job_mapping = job_mapping.clone();
                    let job_path_mapping = job_path_mapping.clone();

                    tokio::task::spawn(async move {
                        match Self::add_job(
                            scheduler,
                            source.to_string(),
                            new_job,
                            job_mapping,
                            job_path_mapping,
                        )
                        .await
                        {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error adding job: {}", e);
                                return;
                            }
                        }
                    });

                    path_mapping.insert(path.clone(), last_modified);
                }
            }
        }
    }

    async fn add_job(
        scheduler: Arc<JobScheduler>,
        path: String,
        job: Job,
        job_mapping: Arc<Mutex<HashMap<String, uuid::Uuid>>>,
        job_path_mapping: Arc<Mutex<HashMap<String, uuid::Uuid>>>,
    ) -> Result<(), Error> {
        let cron_schedule = job.cron_schedule.clone();
        let job_name = job.name.clone();

        let rcron_job = RcronJob::new_async_tz(
            cron_schedule.as_str(),
            FixedOffset::east_opt(7 * 3600).unwrap(),
            move |uuid, _l| {
                let job = job.clone();
                Box::pin(async move {
                    let mut job = job.clone();
                    let job_name = job.name.clone();

                    let current_timestamp = chrono::Local::now().timestamp();
                    let mut envs = HashMap::new();
                    envs.insert("RFLOWTS".to_string(), current_timestamp.to_string());

                    info!(
                        "Running job {} with uuid {} at {}",
                        job_name,
                        uuid,
                        chrono::Local::now()
                    );

                    let result = job.run_with_envs(envs).await;
                    if let Err(e) = result {
                        error!(
                            "Error running job {} - {} with error: {}",
                            job_name, uuid, e
                        );
                    }
                })
            },
        )?;

        let guid = rcron_job.guid();

        scheduler.add(rcron_job).await?;

        job_mapping.lock().await.insert(job_name, guid.clone());
        job_path_mapping
            .lock()
            .await
            .insert(path.clone(), guid.clone());

        Ok(())
    }
}

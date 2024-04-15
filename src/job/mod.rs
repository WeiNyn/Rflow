use crate::{notifier::ZaloNotifier, task};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use failure::{format_err, Error};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use toml::from_str;

pub fn default_notifier() -> ZaloNotifier {
    ZaloNotifier {
        group_id: 417139620,
        sender_id: 210965174,
        owner_id: 217856628,
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TaskConfig {
    pub name: String,
    #[serde(default = "String::new")]
    pub description: String,
    pub command: String,
    pub retry_policy: Option<task::RetryPolicy>,
    pub dependencies: Option<Vec<String>>,
    #[serde(default = "task::default_task_type")]
    pub task_type: task::TaskType,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct JobConfig {
    pub name: String,
    pub description: String,
    #[serde(default = "HashMap::new")]
    pub environments: HashMap<String, String>,
    pub tasks: Vec<TaskConfig>,
    pub cron_schedule: String,
    #[serde(default = "HashMap::new")]
    pub meta_data: HashMap<String, String>,
    pub stdout: String,
    pub stderr: String,
    #[serde(default = "default_notifier")]
    pub notifier: ZaloNotifier,
}

#[derive(Clone)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub description: String,
    pub environments: HashMap<String, String>,
    pub tasks: Vec<Arc<tokio::sync::Mutex<task::Task>>>,
    pub cron_schedule: String,
    pub meta_data: HashMap<String, String>,
    pub stdout: String,
    pub stderr: String,
    pub notifier: ZaloNotifier,
}

impl Job {
    pub fn new(config: JobConfig) -> Result<Job, Error> {
        let id = format!(
            "{:x}",
            md5::compute(serde_json5::to_string(&config).unwrap())
        );

        let mut tasks = Vec::new();
        let task_names = config
            .tasks
            .iter()
            .map(|task| task.name.clone())
            .collect::<HashSet<String>>();

        for task_config in config.tasks {
            let mut task = task::Task::new(
                task_config.name,
                task_config.description,
                task_config.command,
                task_config.task_type,
            );

            task.set_stdout(config.stdout.clone());
            task.set_stderr(config.stderr.clone());
            for (key, value) in config.environments.iter() {
                task.set_env(key.clone(), value.clone());
            }

            if let Some(retry_policy) = task_config.retry_policy {
                task.set_retry_policy(retry_policy);
            }

            if let Some(dependencies) = task_config.dependencies {
                for dep in dependencies {
                    if !task_names.contains(&dep) {
                        return Err(format_err!("Task {} not found", dep));
                    } else {
                        task.add_dependency(dep);
                    }
                }
            }

            let task = Arc::new(Mutex::new(task));
            tasks.push(task);
        }

        Ok(Job {
            id,
            name: config.name,
            description: config.description,
            environments: config.environments,
            tasks,
            cron_schedule: config.cron_schedule,
            meta_data: config.meta_data,
            stdout: config.stdout,
            stderr: config.stderr,
            notifier: config.notifier,
        })
    }

    pub fn load_from_toml(path: &str) -> Result<Job, Error> {
        let toml = std::fs::read_to_string(path)?;
        let config: JobConfig = from_str(toml.as_str())?;
        Job::new(config)
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        info!("JOB STARTING [{}]", self.name);

        let msg = format!(
            "[{}] - [RFLOW] - JOB STARTING [{}]",
            chrono::Local::now().format("%m-%d %H:%M:%S"),
            self.name
        );
        self.notifier.send_msg(&msg).await?;

        debug!("Clear all tasks status");
        for task in self.tasks.iter() {
            let mut task = task.lock().await;
            task.clear_status();
        }

        let mut successed_tasks = HashSet::new();
        let mut failed_tasks = HashSet::new();
        let mut pendding_tasks = Vec::new();
        for task in self.tasks.iter() {
            pendding_tasks.push(task.lock().await.get_name());
        }

        loop {
            let mut spawned_tasks = Vec::new();

            for task in self.tasks.iter() {
                let cur_task = task.lock().await;
                let task_name = cur_task.get_name();
                let task_status = cur_task.get_status();

                if task_status == task::TaskStatus::Success {
                    successed_tasks.insert(task_name.clone());
                    pendding_tasks.retain(|x| x != &task_name);
                    continue;
                }

                if task_status == task::TaskStatus::Failed {
                    error!("Task {} failed", task_name);
                    failed_tasks.insert(task_name.clone());
                    pendding_tasks.retain(|x| x != &task_name);
                    continue;
                }

                let dependencies_status = cur_task.check_dependencies(
                    successed_tasks.iter().cloned().collect::<Vec<String>>(),
                    failed_tasks.iter().cloned().collect::<Vec<String>>(),
                );

                let job_name = self.name.clone();
                match dependencies_status {
                    task::DependenciesStatus::Success => {
                        info!("[{}] - [{}] START", job_name, task_name);

                        let task = task.clone();
                        let spawned_task = tokio::spawn(async move {
                            let mut task = task.lock().await;

                            let retry_policy = task.get_retry_policy();
                            let mut num_retries = 0;
                            loop {
                                let start = tokio::time::Instant::now();
                                let result = task.execute().await;
                                let duration = start.elapsed();

                                match result {
                                    Ok(_) => {
                                        info!(
                                            "[{}] - [{}] SUCCESS after {:?}",
                                            job_name,
                                            task.get_name(),
                                            duration
                                        );
                                        break;
                                    }
                                    Err(e) => {
                                        error!(
                                            "[{}] - [{}] FAILED for {} times: {}",
                                            job_name,
                                            task.get_name(),
                                            num_retries + 1,
                                            e
                                        );
                                        if num_retries >= retry_policy.max_retries {
                                            break;
                                        }
                                        num_retries += 1;
                                        tokio::time::sleep(Duration::from_secs(retry_policy.delay))
                                            .await;
                                    }
                                }

                                if num_retries >= retry_policy.max_retries {
                                    error!(
                                        "[{}] - [{}] FAILED after {} retries",
                                        job_name,
                                        task.get_name(),
                                        num_retries
                                    );
                                    break;
                                }
                            }
                        });

                        spawned_tasks.push(spawned_task);
                    }
                    task::DependenciesStatus::Failed => {
                        error!("[{}] - [{}] dependencies FAILED", job_name, task_name);
                        failed_tasks.insert(task_name.clone());
                        pendding_tasks.retain(|x| x != &task_name);
                        continue;
                    }
                    task::DependenciesStatus::Pending => {
                        debug!("[{}] - [{}] dependencies pending", job_name, task_name);
                        continue;
                    }
                }
            }

            for spawned_task in spawned_tasks {
                spawned_task.await?;
            }

            if pendding_tasks.is_empty() {
                if failed_tasks.is_empty() {
                    info!("JOB FINISHED [{}]", self.name);

                    let msg = format!(
                        "[{}] - [RFLOW] - JOB SUCCESS [{}]",
                        chrono::Local::now().format("%m-%d %H:%M:%S"),
                        self.name
                    );
                    self.notifier.send_msg(&msg).await?;
                } else {
                    error!("JOB FAILED [{}]", self.name);

                    let msg = format!(
                        "[{}] - [RFLOW] - JOB FAILED [{}]",
                        chrono::Local::now().format("%m-%d %H:%M:%S"),
                        self.name
                    );
                    self.notifier.send_msg(&msg).await?;
                }
                break;
            }
        }

        Ok(())
    }

    pub async fn run_with_envs(&mut self, envs: HashMap<String, String>) -> Result<(), Error> {
        for task in self.tasks.iter() {
            let mut task = task.lock().await;
            for (key, value) in envs.iter() {
                task.set_env(key.clone(), value.clone());
            }
        }

        self.run().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_up() {
        let _ = log4rs::init_file("configs/log4rs.yaml", Default::default());
    }

    #[tokio::test]
    async fn test_job() {
        let job_config = JobConfig {
            name: "test".to_string(),
            description: "test".to_string(),
            environments: HashMap::new(),
            tasks: vec![TaskConfig {
                name: "test".to_string(),
                description: "test".to_string(),
                command: "echo test".to_string(),
                retry_policy: None,
                dependencies: None,
                task_type: task::TaskType::Bash,
            }],
            cron_schedule: "* * * * *".to_string(),
            meta_data: HashMap::new(),
            stdout: "stdout".to_string(),
            stderr: "stderr".to_string(),
            notifier: default_notifier(),
        };

        let mut job = Job::new(job_config).unwrap();
        job.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_job_with_dependencies() {
        let job_config = JobConfig {
            name: "test".to_string(),
            description: "test".to_string(),
            environments: HashMap::new(),
            tasks: vec![
                TaskConfig {
                    name: "test1".to_string(),
                    description: "test1".to_string(),
                    command: "echo $\"(date now) a\"".to_string(),
                    retry_policy: None,
                    dependencies: None,
                    task_type: task::TaskType::Nu,
                },
                TaskConfig {
                    name: "test2".to_string(),
                    description: "test2".to_string(),
                    command: "echo (date now)".to_string(),
                    retry_policy: None,
                    dependencies: Some(vec!["test1".to_string()]),
                    task_type: task::TaskType::Nu,
                },
            ],
            cron_schedule: "* * * * *".to_string(),
            meta_data: HashMap::new(),
            stdout: "stdout".to_string(),
            stderr: "stderr".to_string(),
            notifier: default_notifier(),
        };

        let mut out = String::new();
        job_config
            .serialize(toml::Serializer::new(&mut out))
            .unwrap();

        let mut job = Job::new(job_config).unwrap();
        job.run().await.unwrap();

        println!("{}", out);
    }

    #[tokio::test]
    async fn test_load_from_toml() {
        let config_path = "test_config.toml";
        let mut job = Job::load_from_toml(config_path).unwrap();

        job.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_run_with_envs() {
        let job_config = JobConfig {
            name: "test".to_string(),
            description: "test".to_string(),
            environments: HashMap::new(),
            tasks: vec![TaskConfig {
                name: "test".to_string(),
                description: "test".to_string(),
                command: "echo $TEST".to_string(),
                retry_policy: None,
                dependencies: None,
                task_type: task::TaskType::Bash,
            }],
            cron_schedule: "* * * * *".to_string(),
            meta_data: HashMap::new(),
            stdout: "stdout".to_string(),
            stderr: "stderr".to_string(),
            notifier: default_notifier(),
        };

        let mut job = Job::new(job_config).unwrap();
        let mut envs = HashMap::new();
        envs.insert("TEST".to_string(), "test_env".to_string());
        job.run_with_envs(envs).await.unwrap();
    }
}

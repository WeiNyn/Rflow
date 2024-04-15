use failure::{Error, ResultExt};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub delay: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Success,
    Failed,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum TaskType {
    Bash,
    Nu,
}

pub fn default_task_type() -> TaskType {
    TaskType::Bash
}

impl RetryPolicy {
    pub fn no_retry() -> RetryPolicy {
        RetryPolicy {
            max_retries: 0,
            delay: 0,
        }
    }
}

pub enum DependenciesStatus {
    Pending,
    Success,
    Failed,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Task {
    id: String,
    name: String,
    description: String,
    status: TaskStatus,
    environment: HashMap<String, String>,
    command: String,
    stdout: String,
    stderr: String,
    #[serde(default = "RetryPolicy::no_retry")]
    retry_policy: RetryPolicy,
    #[serde(default = "Vec::new")]
    deps: Vec<String>,
    #[serde(default = "default_task_type")]
    task_type: TaskType,
}

impl Task {
    pub fn new(name: String, description: String, command: String, task_type: TaskType) -> Task {
        let id = Uuid::new_v4().to_string();
        Task {
            id,
            name,
            description,
            status: TaskStatus::Pending,
            environment: HashMap::new(),
            command,
            stdout: String::new(),
            stderr: String::new(),
            retry_policy: RetryPolicy::no_retry(),
            deps: Vec::new(),
            task_type: task_type,
        }
    }

    pub fn load_json(json: String) -> Result<Task, Error> {
        let result = serde_json5::from_str(&json.as_str());

        match result {
            Ok(task) => Ok(task),
            Err(e) => Err(failure::format_err!("failed to load task: {}", e)),
        }
    }

    pub fn set_env(&mut self, key: String, value: String) {
        self.environment.insert(key, value);
    }

    pub fn update_status(&mut self, status: TaskStatus) {
        self.status = status;
    }

    pub fn set_stdout(&mut self, stdout: String) {
        self.stdout = stdout;
    }

    pub fn set_stderr(&mut self, stderr: String) {
        self.stderr = stderr;
    }

    pub fn set_retry_policy(&mut self, retry_policy: RetryPolicy) {
        self.retry_policy = retry_policy;
    }

    pub fn add_dependency(&mut self, task_name: String) {
        self.deps.push(task_name);
    }

    pub fn get_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_description(&self) -> String {
        self.description.clone()
    }

    pub fn get_status(&self) -> TaskStatus {
        self.status.clone()
    }

    pub fn get_environment(&self) -> HashMap<String, String> {
        self.environment.clone()
    }

    pub fn get_command(&self) -> String {
        self.command.clone()
    }

    pub fn get_stdout(&self) -> String {
        self.stdout.clone()
    }

    pub fn get_stderr(&self) -> String {
        self.stderr.clone()
    }

    pub fn get_retry_policy(&self) -> RetryPolicy {
        self.retry_policy.clone()
    }

    pub fn get_dependencies(&self) -> Vec<String> {
        self.deps.clone()
    }

    pub fn clear_status(&mut self) {
        self.status = TaskStatus::Pending;
    }

    pub fn check_dependencies(
        &self,
        success_tasks: Vec<String>,
        failed_tasks: Vec<String>,
    ) -> DependenciesStatus {
        debug!("checking dependencies for task {}", self.get_name());
        debug!("dependencies: {:?}", self.get_dependencies());
        debug!("success tasks: {:?}", success_tasks);
        debug!("failed tasks: {:?}", failed_tasks);
        for dep in self.get_dependencies() {
            if failed_tasks.contains(&dep) {
                return DependenciesStatus::Failed;
            }
        }

        for dep in self.get_dependencies() {
            if !success_tasks.contains(&dep) {
                return DependenciesStatus::Pending;
            }
        }

        DependenciesStatus::Success
    }

    pub async fn execute(&mut self) -> Result<Duration, Error> {
        self.update_status(TaskStatus::Running);
        let now = Instant::now();

        let mut command = match self.task_type {
            TaskType::Bash => Command::new("/bin/bash"),
            TaskType::Nu => Command::new("nu"),
        };

        command.arg("-c").arg(self.get_command());

        for (key, value) in self.get_environment() {
            command.env(key, value);
        }

        let stdout = File::options()
            .write(true)
            .create(true)
            .append(true)
            .open(self.get_stdout())
            .context("failed to open file")?;
        let stderr = File::options()
            .write(true)
            .create(true)
            .append(true)
            .open(self.get_stderr())
            .context("failed to open file")?;

        command.stdout(Stdio::from(stdout));
        command.stderr(Stdio::from(stderr));

        if let Ok(mut child) = command.spawn() {
            let pid = child.id();

            info!("Task {} started with pid {}", self.get_name(), pid);

            let status = child.wait().context("failed to wait on child")?;
            if status.success() {
                debug!(
                    "Task [{}] SUCCESS after [{:?}]",
                    self.get_name(),
                    now.elapsed()
                );
                self.update_status(TaskStatus::Success);
            } else {
                debug!("Task [{}] FAILED", self.get_name());
                self.update_status(TaskStatus::Failed);
                return Err(failure::format_err!(
                    "FAILED WITH EXIT CODE [{:?}]",
                    status.code()
                ));
            }
        } else {
            debug!("Task [{}] FAILED TO START", self.get_name());
            self.update_status(TaskStatus::Failed);
            return Err(failure::format_err!(
                "Task {} failed to start",
                self.get_name()
            ));
        }

        let elapsed = now.elapsed();
        Ok(elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_task() {
        let task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echo test".to_string(),
            TaskType::Bash,
        );
        assert_eq!(task.get_name(), "test");
        assert_eq!(task.get_description(), "test");
        assert_eq!(task.get_command(), "echo test");
    }

    #[tokio::test]
    async fn test_set_env() {
        let mut task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echo test".to_string(),
            TaskType::Bash,
        );
        task.set_env("test".to_string(), "test".to_string());
        assert_eq!(task.get_environment().get("test").unwrap(), "test");
    }

    #[tokio::test]
    async fn test_update_status() {
        let mut task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echo test".to_string(),
            TaskType::Bash,
        );
        task.update_status(TaskStatus::Running);
        assert_eq!(task.get_status(), TaskStatus::Running);
    }

    #[tokio::test]
    async fn test_execute() {
        let mut task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echo test".to_string(),
            TaskType::Bash,
        );
        task.set_stdout("test.txt".to_string());
        task.set_stderr("test.txt".to_string());
        task.execute().await;
        assert_eq!(task.get_status(), TaskStatus::Success);
    }

    #[tokio::test]
    async fn test_get_id() {
        let task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echo test".to_string(),
            TaskType::Bash,
        );
        assert_eq!(task.get_id().len(), 36);
    }

    #[tokio::test]
    async fn test_get_stdout() {
        let mut task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echo test".to_string(),
            TaskType::Bash,
        );
        task.set_stdout("test.txt".to_string());
        assert_eq!(task.get_stdout(), "test.txt");
    }

    #[tokio::test]
    async fn test_get_stderr() {
        let mut task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echo test".to_string(),
            TaskType::Bash,
        );
        task.set_stderr("test.txt".to_string());
        assert_eq!(task.get_stderr(), "test.txt");
    }

    #[tokio::test]
    async fn test_get_status() {
        let task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echp test".to_string(),
            TaskType::Bash,
        );
        assert_eq!(task.get_status(), TaskStatus::Pending);
    }

    #[tokio::test]
    async fn test_get_environment() {
        let mut task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echp test".to_string(),
            TaskType::Bash,
        );
        task.set_env("test".to_string(), "test".to_string());
        assert_eq!(task.get_environment().get("test").unwrap(), "test");
    }

    #[tokio::test]
    async fn test_get_command() {
        let task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "echp test".to_string(),
            TaskType::Bash,
        );
        assert_eq!(task.get_command(), "echo test");
    }

    #[tokio::test]
    async fn test_execute_failed() {
        let mut task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "ls /test".to_string(),
            TaskType::Bash,
        );
        task.set_stdout("test.txt".to_string());
        task.set_stderr("test.txt".to_string());
        task.execute().await;
        assert_eq!(task.get_status(), TaskStatus::Failed);
    }

    #[tokio::test]
    async fn test_long_running_command() {
        let mut task = Task::new(
            "test".to_string(),
            "test".to_string(),
            "sleep 5".to_string(),
            TaskType::Bash,
        );
        task.set_stdout("test.txt".to_string());
        task.set_stderr("test.txt".to_string());
        task.execute().await;
        assert_eq!(task.get_status(), TaskStatus::Success);
    }
}

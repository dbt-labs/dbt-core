# Building a Mature Analytics Workflow: The dbt Viewpoint

> In 2015-2016, a team of folks at RJMetrics had the opportunity to observe, and participate in, a significant evolution of the analytics ecosystem. The seeds of dbt were conceived in this environment, and the viewpoint below was written to reflect what we had learned and how we believed the world should be different. **dbt is our attempt to address the workflow challenges we observed, and as such, this viewpoint is the most foundational statement of the dbt project's goals.**

> The remainder of this document is largely unedited from [the original post](https://medium.com/analyst-collective/building-a-mature-analytics-workflow-the-fishtown-analytics-viewpoint-7653473ef05b).

## Analytics today

The center of gravity in mature analytics organizations has shifted away from proprietary, end-to-end tools towards more composable solutions made up of:

- data integration scripts and/or tools,
- high-performance analytic databases,
- SQL, R, and/or Python, and
- visualization tools.

This change has unlocked significant possibility, but analytics teams (ours included) have still faced challenges in consistently delivering high-quality, low-latency analytics.

We believe that analytics teams have a workflow problem. Too often, analysts operate in isolation, and this creates suboptimal outcomes. Knowledge is siloed. We too often rewrite analyses that a colleague had already written. We fail to grasp the nuances of datasets that we’re less familiar with. We differ in our calculations of a shared metric.

We have convinced ourselves after hundreds of conversations that these conditions are by and large the status quo for even sophisticated analytics teams. As a result, organizations suffer from reduced decision speed and reduced decision quality.

Analytics doesn’t have to be this way. In fact, the playbook for solving these problems already exists — on our software engineering teams.
The same techniques that software engineering teams use to collaborate on the rapid creation of quality applications can apply to analytics. We believe it’s time to build an open set of tools and processes to make that happen.

## Analytics is collaborative
We believe a mature analytics team’s techniques and workflow should have the following collaboration features:

### Version Control
Analytic code — whether it’s Python, SQL, Java, or anything else — should be version controlled. Analysis changes as data and businesses evolve, and it’s important to know who changed what, when.

### Quality Assurance
Bad data can lead to bad analyses, and bad analyses can lead to bad decisions. Any code that generates data or analysis should be reviewed and tested.

### Documentation
Your analysis is a software application, and, like every other software application, people are going to have questions about how to use it. Even though it might seem simple, in reality the “Revenue” line you’re showing could mean dozens of things. Your code should come packaged with a basic description of how it should be interpreted, and your team should be able to add to that documentation as additional questions arise.

### Modularity
If you build a series of analyses about your company’s revenue, and your colleague does as well, you should use the same input data. Copy-paste is not a good approach here — if the definition of the underlying set changes, it will need to be updated everywhere it was used. Instead, think of the schema of a data set as its public interface. Create tables, views, or other data sets that expose a consistent schema and can be modified if business logic changes.

## Analytic code is an asset
The code, processes, and tooling required to produce that analysis are core organizational investments. We believe a mature analytics organization’s workflow should have the following characteristics so as to protect and grow that investment:

### Environments
Analytics requires multiple environments. Analysts need the freedom to work without impacting users, while users need service level guarantees so that they can trust the data they rely on to do their jobs.

### Service level guarantees
Analytics teams should stand behind the accuracy of all analysis that has been promoted to production. Errors should be treated with the same level of urgency as bugs in a production product. Any code being retired from production should go through a deprecation process.

### Design for maintainability
Most of the cost involved in software development is in the maintenance phase. Because of this, software engineers write code with an eye towards maintainability.

Analytic code, however, is often fragile. Changes in underlying data break most analytic code in ways that are hard to predict and to fix.
Analytic code should be written with an eye towards maintainability. Future changes to the schema and data should be anticipated and code should be written to minimize the corresponding impact.

## Analytics workflows require automated tools
Frequently, much of an analytic workflow is manual. Piping data from source to destination, from stage to stage, can eat up a majority of an analyst’s time. Software engineers build extensive tooling to support the manual portions of their jobs. In order to implement the analytics workflows we are suggesting, similar tools will be required.

Here’s one example of an automated workflow:

- models and analysis are downloaded from multiple source control repositories,
- code is configured for the given environment,
- code is tested, and
- code is deployed.

Workflows like this should be built to execute with a single command.

import seaborn as sns
import matplotlib
matplotlib.use("Agg")  # headless plotting inside Docker
import matplotlib.pyplot as plt


def display_ecs(df):
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df.sort_values("ecs", ascending=False), x="community", y="ecs", color="#4c78a8")
    plt.xticks(rotation=90)
    plt.ylabel("ECS (cohesion × separation)")
    plt.title("Echo Chamber Score by Community")
    plt.tight_layout()
    plt.savefig("ecs_by_community.png")
    plt.close()


def display_conductance(df):
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df.sort_values("conductance", ascending=True), x="community", y="conductance", color="#f58518")
    plt.xticks(rotation=90)
    plt.ylabel("Conductance (lower = more insulated)")
    plt.title("Conductance by Community")
    plt.tight_layout()
    plt.savefig("conductance_by_community.png")
    plt.close()


def display_homophily(df):
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df.sort_values("homophily", ascending=True), x="community", y="homophily", color="#54a24b")
    plt.xticks(rotation=90)
    plt.ylabel("Homophily")
    plt.title("Homophily by Community")
    plt.tight_layout()
    plt.savefig("homophily_by_community.png")
    plt.close()

def display_variance(df):
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df.sort_values("variance", ascending=True), x="community", y="variance", color="#54a24b")
    plt.xticks(rotation=90)
    plt.ylabel("Mean squared distance to centroid")
    plt.title("Embedding Variance by Community")
    plt.tight_layout()
    plt.savefig("variance_by_community.png")
    plt.close()
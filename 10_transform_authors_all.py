import argparse

import arc_endpoints
import requests


class Arc2ArcAuthor:
    """
    Usage: Copy all author ids from source organization to target organization (production environments).
    Cannot be used to copy author photos.

    Example Terminal Command:
    python path-to-file.py --from-org devtraining  --to-org cetest   --from-token devtraining_prod_token  --to-token cetest_prod_token   --dry-run 1 --test-run 5
    """
    def __init__(self, author_id, from_org, to_org, source_auth, target_auth, dry_run):
        self.dry_run = bool(int(dry_run))
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org
        self.author_id = author_id
        self.ans = {}
        self.message = ""

    def doit(self):
        if self.dry_run:
            print("THIS IS A TEST RUN. AUTHOR WILL NOT BE CREATED OR UPDATED.")

        author_exists = requests.get(
            arc_endpoints.get_author_url(self.to_org),
            headers=arc_auth_header_target,
            params={"_id": self.author_id},
        )
        if author_exists.ok:
            self.message = f"{author_exists} {self.author_id} author already exists in {self.to_org} {author_exists.json()}"
            return self.message, None

        author_res = requests.get(
            arc_endpoints.get_author_url(self.from_org),
            headers=arc_auth_header_source,
            params={"_id": self.author_id},
        )
        # This will copy the exact data from the original org into the new org.
        # The author photo will STAY in the same folder of the AWS author photo account, under the original org's name
        # A person has to MANUALLY open the author page and click the "update image" button in the UI to move the photo into the folder with the new org's name
        if not self.dry_run:
            if author_res.ok:
                author_res2 = requests.post(
                    arc_endpoints.get_author_url(self.to_org, "v2"),
                    headers=arc_auth_header_target,
                    json=author_res.json(),
                )
                self.message = (
                    f"{author_res2} {self.to_org} {self.author_id} {author_res2.json()}"
                )
                self.ans = author_res.json()
            else:
                self.message = (
                    f"{author_res} {self.to_org} {self.author_id} {author_res.text}"
                )
            return self.message, self.ans
        else:
            self.message = (
                f"{author_res} {self.to_org} {self.author_id} {author_res.text}"
            )
            return self.message, None


class ProcessArc2ArcAuthors:
    def __init__(self, from_org, to_org, source_auth, target_auth, dry_run, test_run):
        self.dry_run = bool(int(dry_run))
        self.test_run = test_run
        self.arc_auth_header_source = source_auth
        self.arc_auth_header_target = target_auth
        self.from_org = from_org
        self.to_org = to_org

    def doit(self):
        authors_req = requests.get(
            arc_endpoints.get_all_authors_url(self.from_org),
            headers=arc_auth_header_source,
        )
        authors = authors_req.json()["q_results"]
        for index, a in enumerate(authors):
            if self.test_run and int(self.test_run) and index > int(self.test_run):
                break
            result = Arc2ArcAuthor(
                author_id=a["_id"],
                from_org=args.org,
                to_org=args.to_org,
                source_auth=arc_auth_header_source,
                target_auth=arc_auth_header_target,
                dry_run=args.dry_run,
            ).doit()
            print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-org",
        dest="org",
        required=True,
        default="",
        help="source organization id value; org for production or sandbox.org for sandbox'",
    )
    parser.add_argument(
        "--to-org",
        dest="to_org",
        required=True,
        default="",
        help="target organization id value; org for production or sandbox.org for sandbox'",
    )
    parser.add_argument(
        "--from-token",
        dest="from_token",
        required=True,
        default="",
        help="source organization bearer token; production environment'",
    )
    parser.add_argument(
        "--to-token",
        dest="to_token",
        required=True,
        default="",
        help="target organization bearer token; production environment'",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        required=False,
        default=1,
        help="Set this to 1 to test the results of transforming an object. The object will not actually post to the target org.",
    )
    parser.add_argument(
        "--test-run",
        dest="test_run",
        required=False,
        default=5,
        help="Stop processing after this number of loop iterations. dry_run should be 0 (False)",
    )
    args = parser.parse_args()

    arc_auth_header_source = {"Authorization": f"Bearer {args.from_token}"}
    arc_auth_header_target = {"Authorization": f"Bearer {args.to_token}"}

    ProcessArc2ArcAuthors(
        from_org=args.org,
        to_org=args.to_org,
        source_auth=arc_auth_header_source,
        target_auth=arc_auth_header_target,
        dry_run=args.dry_run,
        test_run=args.test_run,
    ).doit()



from __future__ import annotations
 
 
def list_prompt_defs() -> list[dict]:

    return [

        {

            "name": "due_diligence_assessment",

            "description": "Complete due diligence assessment for a company",

            "arguments": [{"name": "company_id", "required": True}],

        },

        {

            "name": "ic_meeting_prep",

            "description": "Prepare Investment Committee meeting package",

            "arguments": [{"name": "company_id", "required": True}],

        },
        {
            "name": "lp_update_letter",
            "description": "Generate an LP-ready portfolio update letter",
            "arguments": [{"name": "company_id", "required": True}],
        },

    ]
 
 
def get_prompt(name: str, arguments: dict) -> list[dict]:

    company_id = arguments["company_id"]
 
    if name == "due_diligence_assessment":

        return [

            {

                "role": "user",

                "content": (

                    f"Perform due diligence for {company_id}.\n"

                    "1. Calculate Org-AI-R score using calculate_org_air_score\n"

                    "2. For dimensions below 60, use generate_justification\n"

                    "3. Run gap analysis with target_org_air=75\n"

                    "4. Project EBITDA impact"

                ),

            }

        ]
 
    if name == "ic_meeting_prep":

        return [

            {

                "role": "user",

                "content": (

                    f"Prepare an IC meeting summary for {company_id} using scoring, "

                    "evidence, justifications, value creation outputs, and generate_ic_memo."

                ),

            }

        ]

    if name == "lp_update_letter":

        return [

            {

                "role": "user",

                "content": (

                    f"Prepare an LP update from the portfolio context associated with {company_id}. "

                    "Use get_portfolio_summary, get_investment_tracker_summary, recall_company_memory, and generate_lp_letter."

                ),

            }

        ]
 
    return []
 

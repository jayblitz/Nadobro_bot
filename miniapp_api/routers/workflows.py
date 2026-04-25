"""Workflow Copilot routes backed by n8n-safe Nadobro services."""

from fastapi import APIRouter
from pydantic import BaseModel, Field

from miniapp_api.dependencies import AuthUser
from src.nadobro.services.async_utils import run_blocking
from src.nadobro.services.workflow_service import (
    build_and_save_workflow,
    get_workflow,
    list_workflow_templates,
)

router = APIRouter()


class WorkflowBuildRequest(BaseModel):
    prompt: str = Field(..., min_length=3, max_length=1000)
    deploy: bool = False


@router.get("/workflows/templates")
async def workflow_templates(user: AuthUser):
    return {"templates": list_workflow_templates()}


@router.post("/workflows/build")
async def workflow_build(body: WorkflowBuildRequest, user: AuthUser):
    return await run_blocking(build_and_save_workflow, user.telegram_id, body.prompt, body.deploy)


@router.get("/workflows/{workflow_id}")
async def workflow_get(workflow_id: str, user: AuthUser):
    workflow = await run_blocking(get_workflow, user.telegram_id, workflow_id)
    return {"workflow": workflow}
